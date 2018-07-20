use Mix.Config

config :test_queue, TestQueue.Core,
  timeout: 120_000,
  folder: "prod_dets/"
