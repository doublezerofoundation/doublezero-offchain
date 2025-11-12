defmodule Scheduler.Worker.CalculateDistribution do
  @moduledoc """
    GenServer that runs at a configured interval (config.exs) to calculate distribution for a DZ epoch
  """
  use GenServer

  require Logger

  def start_link(_var \\ []) do
    state = %{epoch_to_calculate_distribution: nil}
    GenServer.start_link(__MODULE__, state, name: __MODULE__)
  end

  @impl GenServer
  def init(state) do
    # TODO: figure out which distribution should be calculated
    {:ok, state, {:continue, :calculate_distribution}}
  end

  @impl GenServer
  def handle_continue(:calculate_distribution, state) do
    Scheduler.DoubleZero.calculate_distribution(state.current_epoch, ledger_rpc(), solana_rpc())
    {:noreply, state}
  end

  @impl GenServer
  def handle_info(msg, state) do
    Logger.warning("Received unexpected msg: #{msg}")
    {:noreply, state}
  end

  defp ledger_rpc do
    Application.get_env(:scheduler, :ledger_rpc)
  end

  defp solana_rpc do
    Application.get_env(:scheduler, :solana_rpc)
  end
end
