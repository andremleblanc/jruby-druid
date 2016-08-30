module Druid
  module Writer
    class Base
      include TopLevelPackages
      include Druid::Queries::Metadata

      attr_accessor :tranquilizers
      attr_reader :config, :broker

      def initialize(config, broker)
        @config = config
        @broker = broker
        @tranquilizers = []
      end

      def remove_tranquilizer_for_datasource(datasource)
        tranquilizer = tranquilizer_for_datasource(datasource)
        remove_tranquilizer(tranquilizer) if tranquilizer
      end

      def write_point(datasource, datapoint)
        datapoint = Druid::Writer::Tranquilizer::Datapoint.new(datapoint)
        sender = get_tranquilizer(datasource, datapoint)
        send(sender, datapoint)
      end

      private

      def build_tranquilizer(datasource, datapoint)
        current_schema = most_recent_segment_metadata(datasource)

        puts "current_schema: #{current_schema}"

        Druid::Writer::Tranquilizer::Base.new(
          config: config,
          datasource: datasource,
          dimensions: build_dimensions(current_schema, datapoint),
          metrics: build_metrics(current_schema, datapoint)
        )
      end

      def build_dimensions(current_schema, datapoint)
        current_dimensions = current_schema.present? ? current_schema['columns'].except('__time').keys : []
        puts "built dimensions: #{current_dimensions | datapoint.dimensions.keys}"
        current_dimensions | datapoint.dimensions.keys
      end

      def build_metrics(current_schema, datapoint)
        # TODO: To allow metrics with different aggregator types, use values here
        #       and modify how aggregators are built
        current_metrics = current_schema.present? ? current_schema['aggregators'].keys : []
        puts "built metrics: #{current_metrics | datapoint.metrics.keys}"
        current_metrics | datapoint.metrics.keys
      end

      def current_metrics_schema(tranquilizer)
        aggregators = tranquilizer.rollup.aggregators
        metrics = Java::ScalaCollection::JavaConverters.seqAsJavaListConverter(aggregators).asJava.to_a.map do |metric|
          metric.getFieldName unless metric.is_a? io.druid.query.aggregation.CountAggregatorFactory
        end
      end

      def get_tranquilizer(datasource, datapoint)
        puts "GET TRANQ"
        tranquilizer = tranquilizer_for_datasource(datasource)

        unless has_current_schema?(tranquilizer, datapoint)
          remove_tranquilizer(tranquilizer) if tranquilizer
          tranquilizer = build_tranquilizer(datasource, datapoint)
          tranquilizers << tranquilizer
        end

        tranquilizer
      end

      def has_current_schema?(tranquilizer, datapoint)
        return false unless tranquilizer
        current_dimensions = tranquilizer.rollup.dimensions.specMap["dimensions"].to_a
        current_metrics = current_metrics_schema(tranquilizer)
        datapoint_dimensions = datapoint.dimensions.keys
        datapoint_metrics = datapoint.metrics.except(:count).keys
        puts "current_dimensions: #{current_dimensions}"
        puts "datapoint_dimensions: #{datapoint_dimensions}"
        puts "current_metrics: #{current_metrics}"
        puts "datapoint_metrics: #{datapoint_metrics}"
        (current_dimensions & datapoint_dimensions) == datapoint_dimensions && (current_metrics & datapoint_metrics) == datapoint_metrics
      end

      def remove_tranquilizer(tranquilizer)
        tranquilizers.delete(tranquilizer)
        tranquilizer.stop
      end

      def send(sender, datapoint)
        sender.safe_send(datapoint)
      end

      def tranquilizer_for_datasource(datasource)
        tranquilizers.detect{ |t| t.datasource == datasource }
      end
    end
  end
end
