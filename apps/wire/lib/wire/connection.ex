defmodule Wire.Connection do
  @moduledoc "Handles one PostgreSQL client session over the wire protocol v3."
  require Logger

  # Idle connections hibernate after this timeout, freeing their entire heap.
  @idle_timeout_ms 30_000

  defp new_state do
    %{
      stmts: %{},
      portals: %{},
      error_sync: false
    }
  end

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
          loop(socket, new_state())

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

  defp loop(socket, state) do
    case :gen_tcp.recv(socket, 5, @idle_timeout_ms) do
      {:ok, <<type, len::32>>} ->
        dispatch(socket, state, type, len - 4)

      {:error, :timeout} ->
        enter_hibernate(socket, state)

      {:error, :closed} ->
        :ok

      {:error, r} ->
        Logger.error("Connection: #{inspect(r)}")
        :gen_tcp.close(socket)
    end
  end

  # Unified dispatch: read body from socket, then delegate to dispatch_with_body.
  # This eliminates ~200 lines of duplication between the normal and wake paths.
  defp dispatch(socket, _state, ?X, _body_len) do
    :gen_tcp.close(socket)
  end

  defp dispatch(socket, state, type, body_len) do
    case recv_body(socket, body_len) do
      {:ok, body} -> dispatch_with_body(socket, state, type, body)
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

  defp enter_hibernate(socket, state) do
    case :inet.setopts(socket, active: :once) do
      :ok ->
        :proc_lib.hibernate(__MODULE__, :__wake__, [socket, state])
      {:error, _reason} ->
        :gen_tcp.close(socket)
    end
  end

  @doc false
  def __wake__(socket, state) do
    receive do
      {:tcp, ^socket, data} ->
        :inet.setopts(socket, active: false)
        handle_wake_data(socket, state, data)

      {:tcp_closed, ^socket} ->
        :ok

      {:tcp_error, ^socket, reason} ->
        Logger.warning("Connection: tcp_error during hibernate: #{inspect(reason)}")
        :gen_tcp.close(socket)
    after
      300_000 ->
        :gen_tcp.close(socket)
    end
  end

  defp handle_wake_data(socket, state, data) when byte_size(data) >= 5 do
    <<type, len::32, extra::binary>> = data
    body_len = len - 4

    if body_len < 0 do
      :gen_tcp.close(socket)
    else
      handle_wake_msg(socket, state, type, body_len, extra)
    end
  end

  defp handle_wake_data(socket, state, partial) when byte_size(partial) < 5 do
    case :gen_tcp.recv(socket, 5 - byte_size(partial), 30_000) do
      {:ok, more} -> handle_wake_data(socket, state, <<partial::binary, more::binary>>)
      {:error, _} ->
        :gen_tcp.close(socket)
    end
  end

  defp handle_wake_msg(socket, state, type, body_len, extra) do
    {body, leftover} = if byte_size(extra) >= body_len do
      {binary_part(extra, 0, max(body_len, 0)),
       binary_part(extra, body_len, byte_size(extra) - body_len)}
    else
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
        dispatch_with_body(socket, state, type, body)
        if byte_size(leftover) > 0 do
          handle_wake_data(socket, state, leftover)
        else
          :ok
        end
    end
  end

  # --- Wake path dispatch (body already read) ---

  # Error sync: skip everything except Sync on wake path too
  defp dispatch_with_body(socket, %{error_sync: true} = state, ?S, _body) do
    # Sync resets error state and cleans up unnamed portal
    state = %{state | error_sync: false, portals: Map.delete(state.portals, "")}
    :gen_tcp.send(socket, <<"Z", 5::32, "I">>)
    loop(socket, state)
  end

  defp dispatch_with_body(socket, %{error_sync: true} = state, _type, _body) do
    loop(socket, state)
  end

  defp dispatch_with_body(socket, state, ?Q, body) do
    state = state
      |> put_in([:stmts], Map.delete(state.stmts, ""))
      |> put_in([:portals], Map.delete(state.portals, ""))
    sql = body |> String.trim_trailing(<<0>>) |> String.trim()
    execute_query(socket, sql)
    :erlang.garbage_collect()
    loop(socket, state)
  end

  defp dispatch_with_body(socket, _state, ?X, _body) do
    :gen_tcp.close(socket)
  end

  defp dispatch_with_body(socket, state, ?P, data) do
    {stmt_name, rest} = read_cstring(data)
    {query_sql, rest} = read_cstring(rest)
    {param_oids, _rest} = parse_param_oids(rest)

    cond do
      stmt_name != "" and Map.has_key?(state.stmts, stmt_name) ->
        state = handle_extended_error(socket, "42P05",
          "prepared statement \"#{stmt_name}\" already exists", state)
        loop(socket, state)
      stmt_name != "" and map_size(state.stmts) >= 1024 ->
        state = handle_extended_error(socket, "53000",
          "too many prepared statements", state)
        loop(socket, state)
      true ->
        entry = %{sql: query_sql, param_oids: param_oids}
        state = put_in(state, [:stmts, stmt_name], entry)
        :gen_tcp.send(socket, <<"1", 4::32>>)
        loop(socket, state)
    end
  end

  defp dispatch_with_body(socket, state, ?B, data) do
    {portal_name, rest} = read_cstring(data)
    {stmt_name, rest} = read_cstring(rest)

    case Map.fetch(state.stmts, stmt_name) do
      :error ->
        state = handle_extended_error(socket, "26000",
          "prepared statement \"#{stmt_name}\" does not exist", state)
        loop(socket, state)

      {:ok, stmt} ->
        {_fmt_codes, rest} = parse_format_codes(rest)
        {params, rest} = parse_bind_params(rest)
        {_result_fmt, _rest} = parse_format_codes(rest)

        bound_sql = substitute_params(stmt.sql, params)
        portal = %{stmt_name: stmt_name, params: params, sql: bound_sql}
        state = put_in(state, [:portals, portal_name], portal)
        :gen_tcp.send(socket, <<"2", 4::32>>)
        loop(socket, state)
    end
  end

  defp dispatch_with_body(socket, state, ?D, <<kind, rest::binary>>) do
    {name, _} = read_cstring(rest)
    case kind do
      ?S ->
        case Map.fetch(state.stmts, name) do
          {:ok, stmt} ->
            send_parameter_description(socket, stmt.param_oids)
            describe_statement_result(socket, stmt.sql)
            loop(socket, state)
          :error ->
            state = handle_extended_error(socket, "26000",
              "prepared statement \"#{name}\" does not exist", state)
            loop(socket, state)
        end
      ?P ->
        case Map.fetch(state.portals, name) do
          {:ok, portal} ->
            describe_portal_result(socket, portal.sql)
            loop(socket, state)
          :error ->
            state = handle_extended_error(socket, "34000",
              "portal \"#{name}\" does not exist", state)
            loop(socket, state)
        end
      _ ->
        state = handle_extended_error(socket, "08P01", "invalid Describe target", state)
        loop(socket, state)
    end
  end

  defp dispatch_with_body(socket, state, ?E, data) do
    {portal_name, rest} = read_cstring(data)
    _max_rows = case rest do
      <<n::32>> -> n
      <<n::32, _::binary>> -> n
      _ -> 0
    end

    case Map.fetch(state.portals, portal_name) do
      :error ->
        state = handle_extended_error(socket, "34000",
          "portal \"#{portal_name}\" does not exist", state)
        loop(socket, state)

      {:ok, portal} ->
        case run_query(portal.sql) do
          {:rows, cols, rows, tag} ->
            buf = [
              encode_row_desc(cols),
              Enum.map(rows, &encode_data_row/1),
              encode_complete(tag)
            ]
            :gen_tcp.send(socket, buf)
            loop(socket, state)

          {:command, tag} ->
            :gen_tcp.send(socket, encode_complete(tag))
            loop(socket, state)

          {:error, msg} ->
            state = handle_extended_error(socket, "42601", msg, state)
            loop(socket, state)
        end
    end
  end

  defp dispatch_with_body(socket, state, ?S, _body) do
    # Sync resets error state and cleans up unnamed portal
    state = %{state | error_sync: false, portals: Map.delete(state.portals, "")}
    :gen_tcp.send(socket, <<"Z", 5::32, "I">>)
    loop(socket, state)
  end

  defp dispatch_with_body(socket, state, ?C, <<kind, rest::binary>>) do
    {name, _} = read_cstring(rest)
    state = case kind do
      ?S -> %{state | stmts: Map.delete(state.stmts, name)}
      ?P -> %{state | portals: Map.delete(state.portals, name)}
      _ -> state
    end
    :gen_tcp.send(socket, <<"3", 4::32>>)
    loop(socket, state)
  end

  defp dispatch_with_body(socket, state, ?H, _body) do
    loop(socket, state)
  end

  defp dispatch_with_body(socket, state, _type, _body) do
    loop(socket, state)
  end

  # --- Extended Query Protocol helpers ---

  defp read_cstring(data) do
    case :binary.split(data, <<0>>) do
      [str, rest] -> {str, rest}
      _ -> {"", data}
    end
  end

  defp parse_param_oids(<<num::16, rest::binary>>) do
    parse_oids(rest, num, [])
  end
  defp parse_param_oids(_), do: {[], <<>>}

  defp parse_oids(rest, 0, acc), do: {Enum.reverse(acc), rest}
  defp parse_oids(<<oid::32, rest::binary>>, n, acc), do: parse_oids(rest, n - 1, [oid | acc])
  defp parse_oids(rest, _n, acc), do: {Enum.reverse(acc), rest}

  defp parse_format_codes(<<num::16, rest::binary>>) do
    parse_int16_list(rest, num, [])
  end
  defp parse_format_codes(_), do: {[], <<>>}

  defp parse_int16_list(rest, 0, acc), do: {Enum.reverse(acc), rest}
  defp parse_int16_list(<<v::16, rest::binary>>, n, acc), do: parse_int16_list(rest, n - 1, [v | acc])
  defp parse_int16_list(rest, _n, acc), do: {Enum.reverse(acc), rest}

  defp parse_bind_params(<<num::16, rest::binary>>) do
    parse_params_list(rest, num, [])
  end
  defp parse_bind_params(_), do: {[], <<>>}

  defp parse_params_list(rest, 0, acc), do: {Enum.reverse(acc), rest}
  defp parse_params_list(<<-1::signed-32, rest::binary>>, n, acc) do
    parse_params_list(rest, n - 1, [nil | acc])
  end
  defp parse_params_list(<<vlen::32, val::binary-size(vlen), rest::binary>>, n, acc) do
    parse_params_list(rest, n - 1, [val | acc])
  end
  defp parse_params_list(rest, _n, acc), do: {Enum.reverse(acc), rest}

  # Single-pass parameter substitution to prevent $N-in-values corruption.
  # A multi-pass approach (replace $3, then $2, then $1) would corrupt SQL
  # if a parameter value itself contains "$2" — the second pass would
  # substitute inside the already-replaced value.
  defp substitute_params(sql, params) do
    Regex.replace(~r/\$(\d+)/, sql, fn _match, idx_str ->
      idx = String.to_integer(idx_str)
      case Enum.at(params, idx - 1) do
        nil -> "NULL"
        v when is_binary(v) ->
          cleaned = v
            |> String.replace(<<0>>, "")
            |> String.replace("\\", "\\\\")
            |> String.replace("'", "''")
          "'#{cleaned}'"
        v -> to_string(v)
      end
    end)
  end

  defp send_parameter_description(socket, param_oids) do
    num = length(param_oids)
    oids_bin = for oid <- param_oids, into: <<>>, do: <<oid::32>>
    payload = <<num::16, oids_bin::binary>>
    :gen_tcp.send(socket, <<"t", byte_size(payload) + 4::32, payload::binary>>)
  end

  # Describe for both statements and portals sends NoData.
  # NEVER execute queries during Describe — a DELETE portal Describe
  # would execute the deletion. Drivers get RowDescription from the
  # actual Execute response instead.
  defp describe_statement_result(socket, _sql) do
    :gen_tcp.send(socket, <<"n", 4::32>>)
  end

  defp describe_portal_result(socket, _sql) do
    :gen_tcp.send(socket, <<"n", 4::32>>)
  end

  defp handle_extended_error(socket, code, msg, state) do
    :gen_tcp.send(socket, encode_error(code, msg))
    %{state | error_sync: true}
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
