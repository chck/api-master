#!/usr/bin/env ruby
#-*- coding:utf-8 -*-
require 'net/https'
require 'yaml'
require 'active_support'
require 'active_support/core_ext'

class SlackWebhooks
  def initialize
    conf = YAML::load_file("../API.yaml")
    token = conf["slack_token"]
    @uri = URI.parse("https://hooks.slack.com/services/#{token}")
    @https = Net::HTTP.new(@uri.host, @uri.port)
    @https.use_ssl = true
  end

  def main
    request = Net::HTTP::Post.new(@uri.path)
    request.body = {text: "test"}.to_json

    res = @https.request(request)
    p res.body
  end
end

sw = SlackWebhooks.new
sw.main
