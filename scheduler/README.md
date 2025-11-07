# Scheduler

Scheduler is an Elixir application designed to automate and manage the lifecycle of debts within a financial system. It schedules, tracks, and processes various stages of debt management, such as creation, payment reminders, overdue notifications, and closure. The scheduler ensures that all debt-related events are handled in a timely and reliable manner, reducing manual intervention and improving operational efficiency.

## How It Works

The scheduler operates by periodically checking the status of debts and triggering appropriate actions based on predefined rules and schedules. For example, it can send reminders before payment due dates, escalate overdue debts, and mark debts as resolved once payments are completed. The system is designed to be extensible, allowing for the addition of new lifecycle events as business requirements evolve.

## Running the Application

To run the scheduler locally:

1. Ensure you have Elixir installed. You can download it from [elixir-lang.org](https://elixir-lang.org/install.html).
2. Clone this repository and navigate to the project directory.
3. Install dependencies:

   ```sh
   mix deps.get
## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `scheduler` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:scheduler, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/scheduler>.

