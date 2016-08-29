module Druid
  module Queries
    module Metadata
      def datasource_metadata(datasource_name)
        broker.query(
          "queryType" => "dataSourceMetadata",
          "dataSource" => datasource_name
        )
      end

      def most_recent_segment_metadata(datasource_name)
        datasource_metadata = JSON.parse(datasource_metadata(datasource_name))
        if datasource_metadata.present?
          time = datasource_metadata.last['result']['maxIngestedEventTime'].to_time
        end
        interval = time.to_time.strftime("%Y-%m-%d/%Y-%m-") + time.advance(days: 1).day.to_s
        JSON.parse(segment_metadata(datasource_name, [interval])).last
      end

      def segment_metadata(datasource_name, intervals)
        broker.query(
          "queryType" => "segmentMetadata",
          "dataSource" => datasource_name,
          "intervals" => intervals
        )
      end
    end
  end
end
