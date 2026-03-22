defmodule Wire.Connection do
  @moduledoc "Handles one PostgreSQL client session over the wire protocol v3."
  require Logger

  def start(socket) do
    pid = spawn_link(fn -> handshake(socket) end)
    {:ok, pid}
  end

  # --- Startup ---

  defp handshake(socket) do
    # Fix #6: Read type+length in one recv instead of two
    with {:ok, <<len::32>>} <- :gen_tcp.recv(socket, 4),
         {:ok, payload} <- :gen_tcp.recv(socket, len - 4) do
      case payload do
        <<80_877_103::32>> ->
          :gen_tcp.send(socket, "N")
          handshake(socket)

        <<3::16, 0::16, rest::binary>> ->
          params = parse_params(rest)
          Logger.info("connect: #{params["user"]}@#{params["database"]}")
          send_auth_ok(socket)
          send_params(socket)
          send_backend_key(socket)
          send_ready(socket)
          loop(socket)

        _ ->
          send_error(socket, "08P01", "Unsupported protocol")
          :gen_tcp.close(socket)
      end
    else
      {:error, :closed} -> :ok
      {:error, reason} -> Logger.error("Handshake: #{inspect(reason)}")
    end
  end

  defp parse_params(data) do
    data
    |> :binary.split(<<0>>, [:global])
    |> Enum.reject(&(&1 == ""))
    |> Enum.chunk_every(2)
    |> Enum.into(%{}, fn
      [k, v] -> {k, v}
      _ -> {"_", ""}
    end)
  end

  # --- Auth ---

  defp send_auth_ok(s), do: :gen_tcp.send(s, <<"R", 8::32, 0::32>>)

  defp send_params(s) do
    for {k, v} <- [
          {"server_version", "18.0.0"},
          {"server_encoding", "UTF8"},
          {"client_encoding", "UTF8"},
          {"DateStyle", "ISO, MDY"},
          {"integer_datetimes", "on"},
          {"standard_conforming_strings", "on"}
        ] do
      payload = <<k::binary, 0, v::binary, 0>>
      :gen_tcp.send(s, <<"S", byte_size(payload) + 4::32, payload::binary>>)
    end
  end

  defp send_backend_key(s) do
    pid = :erlang.phash2(self(), 0x7FFFFFFF)
    key = :rand.uniform(0x7FFFFFFF)
    :gen_tcp.send(s, <<"K", 12::32, pid::32, key::32>>)
  end

  # --- Query Loop ---
  # Fix #6: Read message type + length in one 5-byte recv

  defp loop(socket) do
    case :gen_tcp.recv(socket, 5) do
      {:ok, <<"Q", len::32>>} ->
        simple_query(socket, len - 4)
        loop(socket)

      {:ok, <<"X", _::32>>} ->
        :gen_tcp.close(socket)

      {:ok, <<"P", len::32>>} ->
        if len > 4, do: :gen_tcp.recv(socket, len - 4)
        :gen_tcp.send(socket, <<"1", 4::32>>)
        loop(socket)

      {:ok, <<"B", len::32>>} ->
        if len > 4, do: :gen_tcp.recv(socket, len - 4)
        :gen_tcp.send(socket, <<"2", 4::32>>)
        loop(socket)

      {:ok, <<"S", len::32>>} ->
        if len > 4, do: :gen_tcp.recv(socket, len - 4)
        :gen_tcp.send(socket, <<"Z", 5::32, "I">>)
        loop(socket)

      {:ok, <<_, len::32>>} ->
        if len > 4, do: :gen_tcp.recv(socket, len - 4)
        loop(socket)

      {:error, :closed} -> :ok
      {:error, r} -> Logger.error("Connection: #{inspect(r)}")
    end
  end

  # --- Simple Query ---

  defp simple_query(socket, body_len) do
    {:ok, data} = :gen_tcp.recv(socket, body_len)
    sql = String.trim_trailing(data, <<0>>)

    # Fix #5: Remove Logger.debug from hot path

    case run_query(String.trim(sql)) do
      {:rows, cols, rows, tag} ->
        # Fix #2: Buffer entire response and send in one write
        buf = [
          encode_row_desc(cols),
          Enum.map(rows, &encode_data_row/1),
          encode_complete(tag),
          <<"Z", 5::32, "I">>
        ]
        :gen_tcp.send(socket, buf)

      {:command, tag} ->
        buf = [encode_complete(tag), <<"Z", 5::32, "I">>]
        :gen_tcp.send(socket, buf)

      {:error, msg} ->
        buf = [encode_error("42601", msg), <<"Z", 5::32, "I">>]
        :gen_tcp.send(socket, buf)
    end
  end

  # --- Query dispatch — routes to Rust engine via NIF ---

  defp run_query(sql) do
    # Fix #6b: Correct normalization order (trim whitespace before trimming semicolon)
    normalized = sql |> String.downcase() |> String.trim() |> String.trim_trailing(";") |> String.trim()

    cond do
      normalized == "select version()" ->
        v = "pgrx 0.1.0 on BEAM/OTP 27 + Rust — PostgreSQL 18.0 compatible"
        {:rows, [{"version", 25}], [[v]], "SELECT 1"}

      normalized == "select current_database()" ->
        {:rows, [{"current_database", 25}], [["pgrx"]], "SELECT 1"}

      normalized == "" ->
        {:command, "EMPTY"}

      true ->
        # Fix #4: NIF returns native Erlang terms, no JSON round-trip
        case Engine.execute_sql(sql) do
          {:ok, %{tag: tag, columns: columns, rows: rows}} ->
            if columns == [] and rows == [] do
              {:command, tag}
            else
              {:rows, columns, rows, tag}
            end

          {:error, msg} ->
            {:error, msg}
        end
    end
  end

  # --- Response Encoding (returns iodata, does NOT send) ---

  defp encode_row_desc(cols) do
    fields =
      for {name, oid} <- cols, into: <<>> do
        <<name::binary, 0, 0::32, 0::16, oid::32, -1::signed-16, -1::signed-32, 0::16>>
      end

    payload = <<length(cols)::16, fields::binary>>
    <<"T", byte_size(payload) + 4::32, payload::binary>>
  end

  defp encode_data_row(vals) do
    fields =
      for v <- vals, into: <<>> do
        case v do
          nil -> <<-1::signed-32>>
          val ->
            bytes = to_string(val)
            <<byte_size(bytes)::32, bytes::binary>>
        end
      end

    payload = <<length(vals)::16, fields::binary>>
    <<"D", byte_size(payload) + 4::32, payload::binary>>
  end

  defp encode_complete(tag) do
    payload = <<tag::binary, 0>>
    <<"C", byte_size(payload) + 4::32, payload::binary>>
  end

  defp encode_error(code, msg) do
    payload = <<"S", "ERROR", 0, "V", "ERROR", 0, "C", code::binary, 0, "M", msg::binary, 0, 0>>
    <<"E", byte_size(payload) + 4::32, payload::binary>>
  end

  defp send_error(s, code, msg) do
    :gen_tcp.send(s, encode_error(code, msg))
  end

  defp send_ready(s), do: :gen_tcp.send(s, <<"Z", 5::32, "I">>)
end
