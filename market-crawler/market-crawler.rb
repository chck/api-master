#!/usr/bin/env ruby
#-*- coding:utf-8 -*-
#http://developer.yahoo.co.jp/webapi/auctions/auction/v2/search.html
require 'rest-client'
require 'active_support'
require 'active_support/core_ext'
require 'yaml'
require 'active_record'
require 'activerecord-import'

$conf = YAML::load_file("./secret.yml")
ActiveRecord::Base.establish_connection($conf["mysql"])
class YahooTickets < ActiveRecord::Base
end

class MarketCrawler
  def initialize(query)
    @query = query
    @url = "http://auctions.yahooapis.jp/AuctionWebService/V2/search"
    @params = {appid: $conf["yahoo"]["appid"], category: $conf["yahoo"]["category"], query: query}
  end

  def get_all_items
    res = get_items()
    item_list = [res["Result"]["Item"]]
    page = 1
    while res["firstResultPosition"].to_i <= res["totalResultsAvailable"].to_i do
      res = get_items(page += 1)
      item_list << res["Result"]["Item"]
    end
    item_list.flatten.compact
  end

  def get_items(page=1) 
    @params[:page] = page
    response = RestClient.get(@url, {params: @params})
    response = Hash.from_xml(response)
    response = response["ResultSet"]
  end

  def clean_title(title)
    title.gsub(@query, "").gsub(/^\s+/,"")
  end
  
  def upsert(item={})
    YahooTickets.new(
      id: item[:id],
      query: @query,
      title: item[:title],
      current_price: item[:current_price],
      bids: item[:bids],
      bid_or_buy: item[:bid_or_buy],
      created_at: DateTime.now
    )
  end

  def bulk_upsert(items_h=[{}])
    items = items_h.map{|item|
      p item[:title]
      YahooTickets.new(
        id: item[:id],
        query: item[:query],
        title: item[:title],
        current_price: item[:current_price],
        bids: item[:bids],
        bid_or_buy: item[:bid_or_buy],
        created_at: item[:created_at]
      )
    }
    YahooTickets.import items.to_a, on_duplicate_key_update: [:title, :current_price, :bids, :bid_or_buy], validate: false
  end

  def write_on_file
    #output_name = Time.now.strftime("%Y%m%d%H%M%S")[2..-1]
    #open(output_name, "w") do |f|
     # puts %w(id query title current_price bids bid_or_buy).join("\t")
     # get_all_items.each do |row|
     #   puts [row["AuctionID"], @query, clean_title(row["Title"]), row["CurrentPrice"], row["Bids"], row["BidOrBuy"]].join("\t")
     # end
  end

  def main
    items = get_all_items.map{|row| {id:row["AuctionID"], query: @query, title:clean_title(row["Title"]), current_price:row["CurrentPrice"], bids: row["Bids"], bid_or_buy:row["BidOrBuy"]}}
    bulk_upsert(items)
    p "success!!: #{items.size}"
  end
end

if __FILE__ == $0
  mc = MarketCrawler.new($conf["query"])
  #loop do
  mc.main
  #end
end
