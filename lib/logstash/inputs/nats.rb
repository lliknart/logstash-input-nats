# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "nats/client"

# .Compatibility Note
# [NOTE]
# ================================================================================
# This plugin does not support SSL authentication yet
# 
# ================================================================================

# This input plugin will read events from a NATS instance; it does not support NATS streaming instance.
# This plugin used the following ruby nats client: https://github.com/nats-io/ruby-nats
# 
# For more information about Nats, see <http://nats.io>
#
# Examples:
#
# [source,ruby]
#   input {
#     # Read events on subject "example" by using an "url" without authentication
#     nats {
#       url => "nats://localhost:4222"
#       subjects => ["example"]
#     }
#   }
#
# [source,ruby]
#   input {
#     # Read events on subject "example" by using an "url" with authentication
#     nats {
#       url => "nats://user:passwd@localhost:4222"
#       subjects => ["example"]
#     }
#   }
#
# [source,ruby]
#   input {
#     # Read events on two subjects by using other paramaters
#     nats {
#       host => "localhost"
#       port => 4222
#       user => "user"
#       pass => "password"
#       subjects => [ "first", "second" ]
#     }
#   }

class LogStash::Inputs::Nats < LogStash::Inputs::Base
  config_name "nats"

  milestone 1 

  # If undefined, Logstash will complain, even if codec is unused.
  default :codec, "json"

  # The url of nats server to connect on
  config :url, :validate => :string

  # The hostname of the nats server
  config :host, :validate => :string, :default => "127.0.0.1"

  # The port to connect on
  config :port, :validate => :number, :default => 4222

  # SSL
  config :ssl, :validate => :boolean, :default => false
  
  # User to authenticate with
  config :user, :validate => :string, :required => false
 
  # Password to authenticate with
  config :pass, :validate => :password, :required => false

  # List of subjects to subscribe on
  config :subjects, :validate => :array, :default => ["logstash"]

  # The queue group to join if needed
  config :queue_group, :validate => :string, :required => false

  # The name of the nats client
  config :name, :validate => :string, :required => false

  # Path of the private key file if ssl is used
  config :private_key_file, :validate => :string, :required => false

  # Path of the certificate file if ssl is used
  config :cert_file, :validate => :string, :required => false

  # Turn on ACK
  config :verbose, :validate => :boolean, :default => false

  # Turns on additional strict format checking
  config :pedantic, :validate => :boolean, :default => false

  # Time to wait before reconnecting
  config :reconnect_time_wait, :validate => :number

  # Number of attempts to connect on nats server
  config :max_reconnect_attempts, :validate => :number

  public
  def register 

    @nats_server = build_nats_server
    
    @nats_config = {
      uri: @nats_server,
      ssl: @ssl,
      pedantic: @pedantic,
      verbose: @verbose,
      reconnect_time_wait: @reconnect_time_wait.nil? ? nil : @reconnect_time_wait.value,
      max_reconnect_attempts: @max_reconnect_attempts.nil? ? nil : @max_reconnect_attempts.value
    }
  end # def register


  def run(queue)
    ['TERM', 'INT'].each { |s| trap(s) {  puts; exit! } }

    NATS.on_error { |err| puts "Server Error: #{err}"; exit! }

    NATS.start(@nats_config) do
      @subjects.each do |subject|
        puts "Listening on [#{subject}]" #unless $show_raw
        NATS.subscribe(subject, :queue => @queue_group ) do |msg, _, sub|
          @codec.decode(msg) do |event|
            decorate(event)
            event.set("nats_subject", sub)
            queue << event
          end
        end
      end
    end
  end

  private 

  def build_nats_server
    if @url.nil?
      if @user.nil? || @pass.nil?
        nats_server = "nats://#{@host}:#{@port}"
      else
        nats_server = "nats://#{@user}:#{@pass.value}@#{@host}:#{@port}"
      end
    else
      @logger.warn("Parameter 'url' is set, ignoring connection parameters: 'host', 'port', 'user' and 'pass'")
      nats_server = @url
    end
    return nats_server
  end  

end # class LogStash::Inputs::Nats
