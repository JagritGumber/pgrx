defmodule Wire.Connection do
  @moduledoc "Handles one PostgreSQL client session over the wire protocol v3."
  require Logger

  # Idle connections hibernate after this timeout, freeing their entire heap.
  @idle_timeout_ms 30_000

  def start(socket) do
    pid = :erlang.spawn_opt(fn -> handshake(socket) end, [
      :link,
      {:min_heap_size, 233},
      {:min_bin_vheap_size, 46},
      {:fullsweep_after, 10}
    ])
    {:ok, pid}
  end

  # --- Startup ---

  defp handshake(socket) do
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
      {:error, reason} ->
        Logger.error("Handshake: #{inspect(reason)}")
        :gen_tcp.close(socket)
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

  defp loop(socket) do
    case :gen_tcp.recv(socket, 5, @idle_timeout_ms) do
      {:ok, <<type, len::32>>} ->
        dispatch(socket, type, len - 4)

      {:error, :timeout} ->
        enter_hibernate(socket)

      {:error, :closed} ->
        :ok

      {:error, r} ->
        Logger.error("Connection: #{inspect(r)}")
        :gen_tcp.close(socket)
    end
  end

  # Unified message dispatch — used by both normal loop and hibernate wake.
  # Reads the body from socket, handles the message, returns to loop.
  defp dispatch(socket, ?Q, body_len) do
    case recv_body(socket, body_len) do
      {:ok, data} ->
        sql = data |> String.trim_trailing(<<0>>) |> String.trim()
        execute_query(socket, sql)
        :erlang.garbage_collect()
        loop(socket)
      :error -> :ok
    end
  end

  defp dispatch(socket, ?X, _body_len) do
    :gen_tcp.close(socket)
  end

  defp dispatch(socket, ?P, body_len) do
    case recv_body(socket, body_len) do
      {:ok, _} -> :gen_tcp.send(socket, <<"1", 4::32>>); loop(socket)
      :error -> :ok
    end
  end

  defp dispatch(socket, ?B, body_len) do
    case recv_body(socket, body_len) do
      {:ok, _} -> :gen_tcp.send(socket, <<"2", 4::32>>); loop(socket)
      :error -> :ok
    end
  end

  defp dispatch(socket, ?S, body_len) do
    case recv_body(socket, body_len) do
      {:ok, _} -> :gen_tcp.send(socket, <<"Z", 5::32, "I">>); loop(socket)
      :error -> :ok
    end
  end

  defp dispatch(socket, _type, body_len) do
    case recv_body(socket, body_len) do
      {:ok, _} -> loop(socket)
      :error -> :ok
    end
  end

  # Safe recv that handles errors instead of crashing
  defp recv_body(_socket, 0), do: {:ok, <<>>}
  defp recv_body(socket, len) do
    case :gen_tcp.recv(socket, len) do
      {:ok, data} -> {:ok, data}
      {:error, :closed} -> :error
      {:error, r} ->
        Logger.error("Connection recv: #{inspect(r)}")
        :gen_tcp.close(socket)
        :error
    end
  end

  # --- Hibernate ---

  defp enter_hibernate(socket) do
    case :inet.setopts(socket, active: :once) do
      :ok ->
        :proc_lib.hibernate(__MODULE__, :__wake__, [socket])
      {:error, _reason} ->
        :gen_tcp.close(socket)
    end
  end

  @doc false
  def __wake__(socket) do
    receive do
      {:tcp, ^socket, data} ->
        :inet.setopts(socket, active: false)
        handle_wake_data(socket, data)

      {:tcp_closed, ^socket} ->
        :ok

      {:tcp_error, ^socket, reason} ->
        Logger.warning("Connection: tcp_error during hibernate: #{inspect(reason)}")
        :gen_tcp.close(socket)
    after
      300_000 ->
        # 5 min safety timeout — prevent zombie processes
        :gen_tcp.close(socket)
    end
  end

  # Reconstruct the 5-byte header from wake data, then feed into
  # the unified dispatch (which reads the body from socket if needed).
  defp handle_wake_data(socket, data) when byte_size(data) >= 5 do
    <<type, len::32, extra::binary>> = data
    body_len = len - 4

    # Guard against malformed packets where len < 4
    if body_len < 0 do
      :gen_tcp.close(socket)
    else
      handle_wake_msg(socket, type, body_len, extra)
    end
  end

  defp handle_wake_data(socket, partial) when byte_size(partial) < 5 do
    case :gen_tcp.recv(socket, 5 - byte_size(partial), 30_000) do
      {:ok, more} -> handle_wake_data(socket, <<partial::binary, more::binary>>)
      {:error, _} ->
        :gen_tcp.close(socket)
    end
  end

  defp handle_wake_msg(socket, type, body_len, extra) do
    {body, leftover} = if byte_size(extra) >= body_len do
      {binary_part(extra, 0, max(body_len, 0)),
       binary_part(extra, body_len, byte_size(extra) - body_len)}
    else
      # Need more bytes from socket
      remaining = body_len - byte_size(extra)
      case :gen_tcp.recv(socket, remaining, 30_000) do
        {:ok, more} -> {<<extra::binary, more::binary>>, <<>>}
        {:error, _} -> {:error, <<>>}
      end
    end

    case body do
      :error ->
        :gen_tcp.close(socket)
      body when is_binary(body) ->
        dispatch_with_body(socket, type, body)
        # Handle coalesced messages
        if byte_size(leftover) > 0 do
          handle_wake_data(socket, leftover)
        else
          :ok  # dispatch_with_body already called loop()
        end
    end
  end

  # Dispatch with body already read (wake path only).
  # The ?Q case needs special handling since we already have the body.
  defp dispatch_with_body(socket, ?Q, body) do
    sql = body |> String.trim_trailing(<<0>>) |> String.trim()
    execute_query(socket, sql)
    :erlang.garbage_collect()
    loop(socket)
  end

  defp dispatch_with_body(socket, ?X, _body) do
    :gen_tcp.close(socket)
  end

  defp dispatch_with_body(socket, ?P, _body) do
    :gen_tcp.send(socket, <<"1", 4::32>>)
    loop(socket)
  end

  defp dispatch_with_body(socket, ?B, _body) do
    :gen_tcp.send(socket, <<"2", 4::32>>)
    loop(socket)
  end

  defp dispatch_with_body(socket, ?S, _body) do
    :gen_tcp.send(socket, <<"Z", 5::32, "I">>)
    loop(socket)
  end

  defp dispatch_with_body(socket, _type, _body) do
    loop(socket)
  end

  # --- Query execution + response encoding ---

  defp execute_query(socket, sql) do
    case run_query(sql) do
      {:rows, cols, rows, tag} ->
        buf = [
          encode_row_desc(cols),
          Enum.map(rows, &encode_data_row/1),
          encode_complete(tag),
          <<"Z", 5::32, "I">>
        ]
        :gen_tcp.send(socket, buf)

      {:command, tag} ->
        :gen_tcp.send(socket, [encode_complete(tag), <<"Z", 5::32, "I">>])

      {:error, msg} ->
        :gen_tcp.send(socket, [encode_error("42601", msg), <<"Z", 5::32, "I">>])
    end
  end

  defp run_query(sql) do
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

  # --- Response Encoding ---

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
