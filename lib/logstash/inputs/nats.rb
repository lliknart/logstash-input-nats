# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "stud/interval"
require "nats/io/client"
require "socket" # for Socket.gethostname

# Generate a repeating message.
#
# This plugin is intented only as an example.

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
  config :subject, :validate => :string, :required => true

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


  public
  def register
    @nats_server = @url.nil? ? "nats://#{@user}:#{@pass}@#{@host}:#{@port}" : "#{@url}"

    @nats = NATS::IO::Client.new
    @nats.connect(:verbose => @verbose,
                  :pedantic => @pedantic,
                  :servers => [@nats_server])
    logger.info("Connected to #{@nats.connected_server}")

    @identity = "#{@nats_url} #{@subject}"
    @logger.info("Registering Nats", :identity => @identity)
  end # def register


  def run(queue)
    received = []
    @nats.subscribe(:subject => @subject,
                    :queue_group => @queue_group) do |msg|
      received << msg

      event = LogStash::Event.new("message" => @message, "host" => @host)
      decorate(event)
      queue << event
      # because the sleep interval can be big, when shutdown happens
      # we want to be able to abort the sleep
      # Stud.stoppable_sleep will frequently evaluate the given block
      # and abort the sleep(@interval) if the return value is true
      Stud.stoppable_sleep(@interval) { stop? }
    end # loop
  end # def run

  def stop
    # nothing to do in this case so it is not necessary to define stop
    # examples of common "stop" tasks:
    #  * close sockets (unblocking blocking reads/accepts)
    #  * cleanup temporary files
    #  * terminate spawned threads
  end

  # private methods ---------------------------
  private

  def nats_params
    if @url.nil?
      connectionParams = {
        :host => @host,
        :port => @port
      }

    else
      @logger.warn("Parameter 'url' is set, ignoring parameters: 'host' and 'port'")
      connectionParams = {
        :servers => @url
      }
    end

    baseParams = {
            :user => @user.nil? ? nil : @user.value,
            :pass => @pass.nil? ? nil : @pass.value,
            :ssl => @ssl,
            :subject => @subject,
            :queue_group => @queue_group.nil? ? nil : @queue_group.value,
            :name => @name.nil? ? nil : @name.value,
            :private_key_file => @private_key_file.nil? ? nil : @private_key_file.value,
            :cert_file => @cert_file.nil? ? nil : @cert_file.value,
            :verbose => @verbose,
            :pedantic => @pedantic
    }

    return connectionParams.merge(baseParams)
  end # nats_params


  def new_nats_instance
    @nats_builder.call
  end

end # class LogStash::Inputs::Nats
