defmodule Cassandra.Error do
  defstruct [:type, :code, :msg, extra: %{}]

  def from_frame(frame) do
    %__MODULE__{
      type: type(frame.code),
      code: frame.code,
      msg: frame.msg
    }
  end

  [
    {:server_error,         0x0000},
    {:protocol_error,       0x000A},
    {:authentication_error, 0x0100},
    {:unavailable,          0x1000},
    {:overloaded,           0x1001},
    {:bootstrapping,        0x1002},
    {:truncate_error,       0x1003},
    {:write_timeout,        0x1100},
    {:write_failure,        0x1500},
    {:read_timeout,         0x1200},
    {:read_failure,         0x1300},
    {:function_failure,     0x1400},
    {:syntax_error,         0x2000},
    {:unauthorized,         0x2100},
    {:invalid,              0x2200},
    {:config_error,         0x2300},
    {:already_exists,       0x2400},
    {:unprepared,           0x2500}
  ]
  |> Enum.each(fn {k, v} ->
    def type(unquote(v)), do: unquote(k)
  end)

end
