require 'support/helpers'

RSpec.shared_examples 'for_query_metadata' do
  describe '#datasource_metadata' do
    let(:datasource_name) { 'widgets' }
    let(:query_args) { { "queryType" => "dataSourceMetadata", "dataSource" => datasource_name } }

    it 'delegates call' do
      expect(subject.broker).to receive(:query).with(query_args)
      subject.datasource_metadata(datasource_name)
    end
  end

  describe '#most_recent_segment_metadata' do
    let(:datasource_metadata) { JSON.generate(datasource_metadata_hash) }
    let(:datasource_metadata_hash) {
      [
        {
          "timestamp" => "2016-08-29T17:42:17.000Z",
          "result" => { "maxIngestedEventTime" => "2016-08-29T17:42:17.000Z" }
        }
      ]
    }

    it 'returns the most recent segment metadata' do
      expect(subject.broker).to receive(:query).twice.and_return(datasource_metadata, '[]')
      subject.most_recent_segment_metadata('foo')
    end
  end

  describe '#segment_metadata' do
    let(:query) {
      {
        "queryType" => "segmentMetadata",
        "dataSource" => datasource,
        "intervals" => intervals
      }
    }
    let(:datasource) { 'foo' }
    let(:intervals) { ['2016-08-01/2016-08-30'] }

    it 'delegates call' do
      expect(subject.broker).to receive(:query).with(query)
      subject.segment_metadata(datasource, intervals)
    end
  end
end
