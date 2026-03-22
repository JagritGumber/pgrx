#!/bin/bash
export PATH="$HOME/.local/share/mise/installs/erlang/27.2/bin:$HOME/.local/share/mise/installs/elixir/1.18.2-otp-27/bin:$PATH"
cd /home/jagrit/pgrx

pkill -9 -f beam.smp 2>/dev/null
sleep 1

# Start pgrx and immediately analyze
mix run -e '
Process.sleep(3000)

IO.puts("============================================")
IO.puts("  pgrx Idle Memory Breakdown")
IO.puts("============================================")
IO.puts("")

# BEAM memory categories
mem = :erlang.memory()
IO.puts("=== BEAM :erlang.memory() ===")
IO.puts("  total:          #{div(mem[:total], 1024)} KB")
IO.puts("  processes:      #{div(mem[:processes], 1024)} KB  (process heaps + stacks)")
IO.puts("  processes_used: #{div(mem[:processes_used], 1024)} KB  (actually used by processes)")
IO.puts("  system:         #{div(mem[:system], 1024)} KB  (everything non-process)")
IO.puts("  atom:           #{div(mem[:atom], 1024)} KB  (atom table)")
IO.puts("  atom_used:      #{div(mem[:atom_used], 1024)} KB  (atoms actually referenced)")
IO.puts("  binary:         #{div(mem[:binary], 1024)} KB  (refc binaries)")
IO.puts("  code:           #{div(mem[:code], 1024)} KB  (loaded BEAM bytecode)")
IO.puts("  ets:            #{div(mem[:ets], 1024)} KB  (ETS tables)")
IO.puts("")

# System info
IO.puts("=== BEAM System Info ===")
IO.puts("  process_count:       #{:erlang.system_info(:process_count)}")
IO.puts("  port_count:          #{:erlang.system_info(:port_count)}")
IO.puts("  schedulers:          #{:erlang.system_info(:schedulers)}")
IO.puts("  dirty_cpu_schedulers: #{:erlang.system_info(:dirty_cpu_schedulers)}")
IO.puts("  dirty_io_schedulers:  #{:erlang.system_info(:dirty_io_schedulers)}")
IO.puts("  wordsize:            #{:erlang.system_info(:wordsize)} bytes")
IO.puts("")

# Per-process breakdown
IO.puts("=== Top Processes by Memory ===")
procs = Process.list()
  |> Enum.map(fn pid ->
    info = Process.info(pid, [:memory, :registered_name, :current_function, :heap_size, :stack_size, :message_queue_len])
    case info do
      nil -> nil
      info ->
        name = case info[:registered_name] do
          [] -> inspect(pid)
          n -> inspect(n)
        end
        {name, info[:memory], info[:heap_size], info[:stack_size], info[:current_function]}
    end
  end)
  |> Enum.reject(&is_nil/1)
  |> Enum.sort_by(fn {_, mem, _, _, _} -> -mem end)
  |> Enum.take(15)

for {name, mem, heap, stack, func} <- procs do
  IO.puts("  #{String.pad_trailing(name, 35)} #{String.pad_leading(Integer.to_string(div(mem, 1024)), 6)} KB  heap=#{heap}w stack=#{stack}w  #{inspect(func)}")
end
IO.puts("")

# NIF / native code
IO.puts("=== Loaded NIF Modules ===")
for {mod, path} <- :code.all_loaded() do
  mod_str = Atom.to_string(mod)
  if String.contains?(mod_str, "Engine") or String.contains?(mod_str, "Native") do
    IO.puts("  #{mod_str} -> #{path}")
  end
end
IO.puts("")

# OS-level memory map analysis
pid = :os.getpid() |> List.to_string()
IO.puts("=== /proc/#{pid}/status ===")
case File.read("/proc/#{pid}/status") do
  {:ok, content} ->
    content
    |> String.split("\n")
    |> Enum.filter(fn line ->
      String.starts_with?(line, "Vm") or String.starts_with?(line, "Rss") or
      String.starts_with?(line, "Threads")
    end)
    |> Enum.each(&IO.puts("  #{&1}"))
  _ -> IO.puts("  (could not read)")
end
IO.puts("")

# smaps_rollup for detailed breakdown
IO.puts("=== /proc/#{pid}/smaps_rollup ===")
case File.read("/proc/#{pid}/smaps_rollup") do
  {:ok, content} ->
    content
    |> String.split("\n")
    |> Enum.filter(fn line ->
      String.contains?(line, "Rss") or String.contains?(line, "Pss") or
      String.contains?(line, "Shared") or String.contains?(line, "Private") or
      String.contains?(line, "Swap") or String.contains?(line, "Anonymous")
    end)
    |> Enum.each(&IO.puts("  #{&1}"))
  _ -> IO.puts("  (could not read)")
end
IO.puts("")

# Count memory mapped regions
IO.puts("=== Memory Map Summary (from /proc/#{pid}/maps) ===")
case File.read("/proc/#{pid}/maps") do
  {:ok, content} ->
    lines = String.split(content, "\n") |> Enum.reject(&(&1 == ""))
    IO.puts("  Total mapped regions: #{length(lines)}")

    # Categorize by type
    categories = Enum.reduce(lines, %{}, fn line, acc ->
      category = cond do
        String.contains?(line, "beam.smp") -> "beam_binary"
        String.contains?(line, "libengine") -> "nif_engine"
        String.contains?(line, "libc") -> "libc"
        String.contains?(line, "libpthread") or String.contains?(line, "librt") -> "threading"
        String.contains?(line, "libm") or String.contains?(line, "libdl") or String.contains?(line, "ld-linux") -> "system_libs"
        String.contains?(line, "libcrypto") or String.contains?(line, "libssl") -> "crypto"
        String.contains?(line, "[heap]") -> "heap"
        String.contains?(line, "[stack") -> "stack"
        String.contains?(line, "[vdso]") or String.contains?(line, "[vvar]") -> "vdso"
        String.contains?(line, ".so") -> "other_shared_libs"
        true -> "anonymous/other"
      end

      # Parse size from address range
      parts = String.split(line, " ", parts: 2)
      addrs = String.split(hd(parts), "-")
      if length(addrs) == 2 do
        {start, _} = Integer.parse(Enum.at(addrs, 0), 16)
        {stop, _} = Integer.parse(Enum.at(addrs, 1), 16)
        size_kb = div(stop - start, 1024)
        Map.update(acc, category, {1, size_kb}, fn {count, total} -> {count + 1, total + size_kb} end)
      else
        acc
      end
    end)

    categories
    |> Enum.sort_by(fn {_, {_, size}} -> -size end)
    |> Enum.each(fn {cat, {count, size_kb}} ->
      IO.puts("  #{String.pad_trailing(cat, 25)} #{String.pad_leading(Integer.to_string(size_kb), 8)} KB  (#{count} regions)")
    end)
  _ -> IO.puts("  (could not read)")
end
IO.puts("")

# Specifically measure NIF .so size
IO.puts("=== NIF Binary Size ===")
nif_path = Path.join([File.cwd!(), "priv", "native", "engine.so"])
case File.stat(nif_path) do
  {:ok, stat} -> IO.puts("  #{nif_path}: #{div(stat.size, 1024)} KB")
  _ -> IO.puts("  NIF not found at #{nif_path}")
end

# Check for release vs debug
for dir <- ["native/engine/target/release", "native/engine/target/debug"] do
  path = Path.join([File.cwd!(), dir, "libengine.so"])
  case File.stat(path) do
    {:ok, stat} -> IO.puts("  #{dir}/libengine.so: #{div(stat.size, 1024)} KB")
    _ -> :ok
  end
end
' 2>/dev/null