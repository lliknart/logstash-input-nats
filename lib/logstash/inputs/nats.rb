# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "stud/interval"
require "nats/io/client"
require "socket" # for Socket.gethostname

# Add any asciidoc formatted documentation here
# Generate a repeating message.
#
# This plugin is intended only as an example.

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
  config :pass, :validate => :string, :required => false

  # The subject name ti subscribe on
  config :subject, :validate => :string, :required => true, :default => "bite"

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

  # TOREMOVE #bite pour s'en rapeller

  config :interval, :validate => :number, :default => 1

  public
  def register
    #@nats_server = @url.nil? ? "nats://#{@user}:#{@pass}@#{@host}:#{@port}" : "#{@url}"
    @nats_server = @url.nil? ? "nats://#{@host}:#{@port}" : "#{@url}"

    @nats = NATS::IO::Client.new
    @nats.connect(:verbose => @verbose,
                  :pedantic => @pedantic,
                  :servers => [@nats_server])
    logger.info("Connected to #{@nats.connected_server}")

    @identity = "#{@nats_url} #{@subject}"
    @logger.info("Registering Nats", :identity => @identity)
  end # def register

  def run(queue)
    @nats.on_error do |e|
      @logger.info("Error: #{e}")
      @logger.info(e.backtrace)
    end

    @nats.on_reconnect do
      @logger.info("Reconnected to server at #{@nats.connected_server}")
    end

    @nats.on_disconnect do
      @logger.info("Disconnected!")
    end

    @nats.on_close do
      @logger.info("Connection to NATS closed")
    end
    Stud.interval(@interval) do
      sid = @nats.subscribe(@subject, :queue => @queue_group) { |msg|
        @logger.info("Received a message: '#{msg}'")
        event = LogStash::Event.new("message" => msg)
        decorate(event)
        queue << event
      }
    end
  end
end # class LogStash::Inputs::Example
