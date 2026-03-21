defmodule Engine.Native do
  use Rustler, otp_app: :engine, crate: "engine", path: "../../native/engine"

  def ping(), do: :erlang.nif_error(:nif_not_loaded)
  def parse_sql(_sql), do: :erlang.nif_error(:nif_not_loaded)
  def parse_sql_ast(_sql), do: :erlang.nif_error(:nif_not_loaded)
end
