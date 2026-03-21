defmodule Engine do
  defdelegate ping, to: Engine.Native
  defdelegate parse_sql(sql), to: Engine.Native
  defdelegate parse_sql_ast(sql), to: Engine.Native
end
