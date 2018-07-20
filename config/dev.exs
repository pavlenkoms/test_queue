use Mix.Config

config :test_queue, TestQueue.Core,
  timeout: 120000,
  folder: "dev_dets/"