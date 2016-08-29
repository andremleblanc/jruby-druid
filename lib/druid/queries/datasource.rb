module Druid
  module Queries
    module Datasource
      include_package 'org.apache.zookeeper'

      ZOOKEEPER_BEAMS_PATH = '/tranquility/beams/druid:overlord'.freeze

      delegate :datasource_enabled?,
               :datasource_info,
               :disable_datasource,
               :list_datasources,
               to: :coordinator

      def datasource_metadata(datasource_name)
        broker.query(
          "queryType" => "dataSourceMetadata",
          "dataSource" => datasource_name
        )
      end

      def delete_datasource(datasource_name)
        shutdown_tasks(datasource_name)
        datasource_enabled?(datasource_name) ? disable_datasource(datasource_name) : true
        writer.remove_tranquilizer_for_datasource(datasource_name)
        delete_zookeeper_nodes(datasource_name) if config.strong_delete
      end

      def delete_datasources
        list_datasources.each{ |datasource_name| delete_datasource(datasource_name) }
      end

      def most_recent_segment_metadata(datasource_name)
        datasource_metadata = JSON.parse(datasource_metadata(datasource_name))
        if datasource_metadata.present?
          time = datasource_metadata.last['result']['maxIngestedEventTime'].to_time
        end
        interval = time.to_time.strftime("%Y-%m-%d/%Y-%m-") + time.advance(days: 1).day.to_s
        segment_metadata(datasource_name, [interval])
      end

      def segment_metadata(datasource_name, intervals)
        broker.query(
          "queryType" => "segmentMetadata",
          "dataSource" => datasource_name,
          "intervals" => intervals
        )
      end

      private

      def delete_zookeeper_nodes(datasource_name)
        curator = Druid::Writer::Tranquilizer::Curator.build(config)
        curator.start
        zk = curator.getZookeeperClient.getZooKeeper
        ZKUtil.deleteRecursive(zk, ZOOKEEPER_BEAMS_PATH + "/#{datasource_name}")
      end
    end
  end
end
