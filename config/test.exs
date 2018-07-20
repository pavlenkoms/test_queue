use Mix.Config

config :test_queue, TestQueue.Core,
  timeout: 1000,
  folder: "test_dets/"

