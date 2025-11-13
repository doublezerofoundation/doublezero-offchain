import Config
alias Scheduler.Worker

config :scheduler, Scheduler.Scheduler,
  jobs: [
    {"0 */2 * * *", {Worker.PayDebt, :start_link, []}},
    {"*/2 * * * *", {Worker.InitializeDistribution, :start_link, []}}
  ]
