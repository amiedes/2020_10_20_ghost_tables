# frozen_string_literal: true

require 'net/http'
require 'byebug'
require 'json'
require 'cgi'

BUILDER_HOST = 'http://carto.localhost.lan:3000'
SQL_API_HOST = 'http://carto.localhost.lan:8080'
API_KEY = '159ea4251d8b42e8f00daa898c152b446051cf9f'

DATASET_RAW_URL = 'https://www.dropbox.com/s/qexlwjnvznpiw3a/dataset_ciudades.csv?dl=1'
TABLE_BASE_NAME = 'dataset_ciudades'

# DATASET_RAW_URL = 'https://www.dropbox.com/s/hisi99qk951rhvj/carto_rmr_ooh_proposal_86.csv?dl=1'
# TABLE_BASE_NAME = 'carto_rmr_ooh_proposal_86'

OLD_TABLE_NAME = TABLE_BASE_NAME
NEW_TABLE_NAME = "#{TABLE_BASE_NAME}_1"
TMP_TABLE_NAME = "#{TABLE_BASE_NAME}_tmp"

def log(message)
  puts "[#{Time.now}] #{message}"
end

def sql_api_get(sql_query)
  uri = URI("#{SQL_API_HOST}/api/v2/sql?q=#{sql_query}&api_key=#{API_KEY}")
  response = Net::HTTP.get(uri)
  response_hash = JSON.parse(response)
  response_hash
end

def list_user_tables
  response = sql_api_get(%(
  SELECT table_name
  FROM information_schema.tables
  WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
  ORDER BY table_name;
  ))
  log "Current DB tables: #{response['rows'].map(&:values).flatten}"
end

def drop_table(table_name)
  log "Dropping table #{table_name}"
  sql_api_get("DROP TABLE #{table_name};")
end

def import_dataset
  uri = URI("#{BUILDER_HOST}/api/v1/imports?api_key=#{API_KEY}&privacy=link")
  response = Net::HTTP.post(
    uri,
    { type_guessing: false, url: DATASET_RAW_URL }.to_json,
    'Content-Type' => 'application/json'
  )
  response_hash = JSON.parse(response.body)
  log "Created import with ID: #{response_hash['item_queue_id']}"
  response_hash
end

def wait_for_import(import_id)
  sleep 10
  uri = URI("#{BUILDER_HOST}/api/v1/imports/#{import_id}?api_key=#{API_KEY}")
  response = Net::HTTP.get(uri)
  response_hash = JSON.parse(response)
  log "Imported #{response_hash['display_name']} as #{response_hash['table_name']}"
end

def count_table_rows(table_name)
  response = sql_api_get("SELECT COUNT(*) FROM #{table_name};")
  #puts "Table #{table_name} has #{response['rows'].first['count']} rows"
  log "Table #{table_name} has #{response['rows']} rows"
end

def rename_table(current_name, new_name)
  response = sql_api_get("ALTER TABLE #{current_name} RENAME TO #{new_name};")
  log "Renamed table #{current_name} to #{new_name}. Extra:"
  log JSON.pretty_generate(response)
end

puts 'INITIAL STATE'
puts '-------------'

list_user_tables

drop_table(OLD_TABLE_NAME)
drop_table(NEW_TABLE_NAME)
drop_table(TMP_TABLE_NAME)

puts "\n\nRUNNING IMPORTS"
puts '-------------'

response = import_dataset
wait_for_import(response['item_queue_id'])

response = import_dataset
wait_for_import(response['item_queue_id'])

count_table_rows(OLD_TABLE_NAME)
count_table_rows(NEW_TABLE_NAME)

puts "\n\nSTART RENAMING"
puts '-------------'

rename_table(OLD_TABLE_NAME, TMP_TABLE_NAME)

list_user_tables

rename_table(NEW_TABLE_NAME, OLD_TABLE_NAME)

list_user_tables

drop_table(TMP_TABLE_NAME)

list_user_tables

sleep(10)

list_user_tables
