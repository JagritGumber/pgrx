defmodule Wire.Connection do
  @moduledoc "Handles one PostgreSQL client session over the wire protocol v3."
  require Logger

  def start(socket) do
    pid = spawn_link(fn -> handshake(socket) end)
    {:ok, pid}
  end

  # --- Startup ---

  defp handshake(socket) do
    with {:ok, <<len::32>>} <- :gen_tcp.recv(socket, 4),
         {:ok, payload} <- :gen_tcp.recv(socket, len - 4) do
      case payload do
        # SSL request (magic 80877103)
        <<80_877_103::32>> ->
          :gen_tcp.send(socket, "N")
          handshake(socket)

        # Protocol v3.0
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

  defp send_ready(s), do: :gen_tcp.send(s, <<"Z", 5::32, "I">>)

  # --- Query Loop ---

  defp loop(socket) do
    case :gen_tcp.recv(socket, 1) do
      {:ok, <<"Q">>} -> simple_query(socket); loop(socket)
      {:ok, <<"X">>} -> :gen_tcp.close(socket)
      {:ok, <<"P">>} -> skip_msg(socket); :gen_tcp.send(socket, <<"1", 4::32>>); loop(socket)
      {:ok, <<"B">>} -> skip_msg(socket); :gen_tcp.send(socket, <<"2", 4::32>>); loop(socket)
      {:ok, <<"D">>} -> skip_msg(socket); loop(socket)
      {:ok, <<"E">>} -> skip_msg(socket); loop(socket)
      {:ok, <<"S">>} -> skip_msg(socket); send_ready(socket); loop(socket)
      {:ok, <<"C">>} -> skip_msg(socket); loop(socket)
      {:ok, <<"H">>} -> skip_msg(socket); loop(socket)
      {:ok, _} -> skip_msg(socket); loop(socket)
      {:error, :closed} -> :ok
      {:error, r} -> Logger.error("Connection: #{inspect(r)}")
    end
  end

  defp skip_msg(socket) do
    {:ok, <<len::32>>} = :gen_tcp.recv(socket, 4)
    if len > 4, do: :gen_tcp.recv(socket, len - 4)
  end

  # --- Simple Query ---

  defp simple_query(socket) do
    {:ok, <<len::32>>} = :gen_tcp.recv(socket, 4)
    {:ok, data} = :gen_tcp.recv(socket, len - 4)
    sql = String.trim_trailing(data, <<0>>)
    Logger.debug("Q: #{sql}")

    case run_query(String.trim(sql)) do
      {:rows, cols, rows, tag} ->
        send_row_desc(socket, cols)
        Enum.each(rows, &send_data_row(socket, &1))
        send_complete(socket, tag)

      {:command, tag} ->
        send_complete(socket, tag)

      {:error, msg} ->
        send_error(socket, "42601", msg)
    end

    send_ready(socket)
  end

  # --- Query dispatch — routes to Rust engine via NIF ---

  defp run_query(sql) do
    normalized = sql |> String.downcase() |> String.trim_trailing(";") |> String.trim()

    # Handle built-in functions that need BEAM-side responses
    cond do
      normalized == "select version()" ->
        v = "pgrx 0.1.0 on BEAM/OTP 27 + Rust — PostgreSQL 18.0 compatible"
        {:rows, [{"version", 25}], [[v]], "SELECT 1"}

      normalized == "select current_database()" ->
        {:rows, [{"current_database", 25}], [["pgrx"]], "SELECT 1"}

      normalized == "" ->
        {:command, "EMPTY"}

      true ->
        case Engine.execute_sql(sql) do
          {:ok, json} ->
            result = Jason.decode!(json)
            columns = Enum.map(result["columns"], fn [name, oid] -> {name, oid} end)
            rows = result["rows"]
            tag = result["tag"]

            if columns == [] and rows == [] do
              {:command, tag}
            else
              text_rows =
                Enum.map(rows, fn row ->
                  Enum.map(row, fn
                    nil -> nil
                    val -> val
                  end)
                end)

              {:rows, columns, text_rows, tag}
            end

          {:error, msg} ->
            {:error, msg}
        end
    end
  end

  # --- Response Encoding ---

  defp send_row_desc(s, cols) do
    fields =
      for {name, oid} <- cols, into: <<>> do
        <<name::binary, 0, 0::32, 0::16, oid::32, -1::signed-16, -1::signed-32, 0::16>>
      end

    payload = <<length(cols)::16, fields::binary>>
    :gen_tcp.send(s, <<"T", byte_size(payload) + 4::32, payload::binary>>)
  end

  defp send_data_row(s, vals) do
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
    :gen_tcp.send(s, <<"D", byte_size(payload) + 4::32, payload::binary>>)
  end

  defp send_complete(s, tag) do
    payload = <<tag::binary, 0>>
    :gen_tcp.send(s, <<"C", byte_size(payload) + 4::32, payload::binary>>)
  end

  defp send_error(s, code, msg) do
    payload = <<"S", "ERROR", 0, "V", "ERROR", 0, "C", code::binary, 0, "M", msg::binary, 0, 0>>
    :gen_tcp.send(s, <<"E", byte_size(payload) + 4::32, payload::binary>>)
  end
end
