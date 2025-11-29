defmodule Scheduler.ValidatorDebt.DebtCollection do
  defstruct total_transactions_attempted: 0,
            outstanding_debt: 0,
            insufficient_funds_count: 0,
            total_paid: 0,
            total_debt: 0
end

defmodule Scheduler.ValidatorDebt.Debt do
  defstruct [:validator_id, :amount, :result, :success]
end
