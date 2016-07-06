module Druid
  module Writer
    class Base
      attr_accessor :tranquilizers
      attr_reader :config

      def initialize(config)
        @config = config
        @tranquilizers = []
      end

      def write_point(datasource, datapoint)
        datapoint = Druid::Writer::Tranquilizer::Datapoint.new(datapoint)
        sender = get_tranquilizer(datasource, datapoint)
        sender.send(datapoint)
      end

      private

      def build_tranquilizer(datasource, datapoint)
        Druid::Writer::Tranquilizer::Base.new({config: config, datasource: datasource, datapoint: datapoint})
      end

      def get_tranquilizer(datasource, datapoint)
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
        dimensions = tranquilizer.rollup.dimensions.specMap["dimensions"].to_a
        aggregators = tranquilizer.rollup.aggregators
        metrics = Java::ScalaCollection::JavaConverters.seqAsJavaListConverter(aggregators).asJava.to_a.map{ |metric| metric.getFieldName }
        dimensions == datapoint.dimensions.keys && metrics == datapoint.metrics.keys
      end

      def remove_tranquilizer(tranquilizer)
        tranquilizers.delete(tranquilizer)
        tranquilizer.stop
      end

      def tranquilizer_for_datasource(datasource)
        tranquilizers.detect{ |t| t.datasource == datasource }
      end
    end
  end
end