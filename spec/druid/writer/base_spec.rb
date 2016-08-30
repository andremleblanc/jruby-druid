require 'spec_helper'

describe Druid::Writer::Base do
  subject { Druid::Writer::Base.new(config, broker) }
  let(:config) { Druid::Configuration.new }
  let(:broker) { Druid::Node::Broker.new(config) }
  let(:datasource_a) { 'writer_base_a' }
  let(:datapoint_1) { { dimensions: dimensions_1, metrics: metrics_1 } }
  let(:datapoint_2) { { dimensions: dimensions_2, metrics: metrics_2 } }
  let(:datapoint_obj_1) { Druid::Writer::Tranquilizer::Datapoint.new(datapoint_1) }
  let(:datapoint_obj_2) { Druid::Writer::Tranquilizer::Datapoint.new(datapoint_2) }
  let(:dimensions_1) { { manufacturer: 'ACME' } }
  let(:dimensions_2) { { owner: 'Wile E. Coyote' } }
  let(:metrics_1) { { anvils: 1 } }
  let(:metrics_2) { { dynamite: 10 } }
  let(:time) { Time.now.utc.beginning_of_hour }
  let(:tranquilizer_1) { Druid::Writer::Tranquilizer::Base.new(tranquilizer_config_1) }
  let(:tranquilizer_2) { Druid::Writer::Tranquilizer::Base.new(tranquilizer_config_2) }
  let(:tranquilizer_config_1) { { config: config, datasource: datasource_a, dimensions: datapoint_obj_1.dimensions, metrics: datapoint_obj_1.metrics } }
  let(:tranquilizer_config_2) { { config: config, datasource: datasource_a, dimensions: datapoint_obj_2.dimensions, metrics: datapoint_obj_2.metrics } }
  let(:n) { 2 }
  let(:next_interval) { Time.now.utc.advance(hours: 1) }

  describe '#remove_tranquilizer_for_datasource' do
    let(:tranquilizer) { double('tranquilizer') }

    context 'when there is a tranquilizer for the datasource' do
      it 'calls remove_tranquilizer with the tranquilizer' do
        expect(subject).to receive(:tranquilizer_for_datasource).with(datasource_a).and_return(tranquilizer)
        expect(subject).to receive(:remove_tranquilizer).with(tranquilizer)
        subject.remove_tranquilizer_for_datasource(datasource_a)
      end
    end

    context 'when there is not a tranquilizer for the datasource' do
      it 'does not call remove_tranquilizer' do
        expect(subject).to receive(:tranquilizer_for_datasource).with(datasource_a)
        expect(subject).not_to receive(:remove_tranquilizer)
        subject.remove_tranquilizer_for_datasource(datasource_a)
      end
    end
  end

  describe '#write_point' do
    context 'writing points to the same datasource' do
      context 'with no schema change' do
        before do
          expect(subject).to receive(:most_recent_segment_metadata).with(datasource_a)
        end

        it 'builds a tranquilzer the first time and then reuse it' do
          expect(Druid::Writer::Tranquilizer::Base).to receive(:new).once.and_call_original
          expect(subject).to receive(:send).exactly(n).times

          n.times { subject.write_point(datasource_a, datapoint_1) }
          expect(subject.tranquilizers.size).to eq 1
        end
      end

      context 'with schema change' do
        let(:metadata) { { 'aggregators' => aggregators, 'columns' => columns } }
        let(:aggregators) { { 'anvils' => { 'type' => 'longSum', name: 'anvils', fieldName: 'anvils'} } }
        let(:columns) { { '__time' => Time.now.utc.iso8601, 'manufacturer' => {} } }

        it 'builds a tranquilizer the first time and reuses it until the schema changes' do
          expect(Druid::Writer::Tranquilizer::Base).to receive(:new).twice.and_call_original
          expect(subject).to receive(:most_recent_segment_metadata).with(datasource_a).and_return({}, metadata)
          expect(subject).to receive(:send).exactly(n * 3).times

          n.times { subject.write_point(datasource_a, datapoint_1) }
          n.times { subject.write_point(datasource_a, datapoint_2) }
          n.times { subject.write_point(datasource_a, datapoint_1) }
          expect(subject.tranquilizers.size).to eq 1
        end
      end

      context 'with no existing tranquilizer' do
        context 'with existing datasource' do
          it 'creates a tranquilizer using schema from datasource' do

          end
        end
      end
    end

    context 'writing points to multiple datasources' do
      let(:metadata) { { 'aggregators' => aggregators } }
      let(:aggregators) { { 'anvils' => { 'type' => 'longSum', name: 'anvils', fieldName: 'anvils'} } }
      let(:datasource_b) { 'writer_base_b' }

      context 'with no schema change' do
        let(:tranquilizer_config_2) { { config: config, datasource: datasource_b, dimensions: datapoint_obj_1.dimensions, metrics: datapoint_obj_1.metrics } }

        it 'builds a tranquilizer for each datasource and reuse them' do
          expect(Druid::Writer::Tranquilizer::Base).to receive(:new).twice.and_call_original
          expect(subject).to receive(:most_recent_segment_metadata).and_return([])
          expect(subject).to receive(:send).exactly(n * 2).times

          n.times { subject.write_point(datasource_a, datapoint_1) }
          n.times { subject.write_point(datasource_b, datapoint_1) }
          expect(subject.tranquilizers.size).to eq 2
        end
      end

      context 'with schema change' do
        let(:tranquilizer_3) { Druid::Writer::Tranquilizer::Base.new(tranquilizer_config_3) }
        let(:tranquilizer_config_3) { { config: config, datasource: datasource_b, dimensions: datapoint_obj_1.dimensions, metrics: datapoint_obj_1.metrics } }

        let(:tranquilizer_4) { Druid::Writer::Tranquilizer::Base.new(tranquilizer_config_4) }
        let(:tranquilizer_config_4) { { config: config, datasource: datasource_b, dimensions: datapoint_obj_2.dimensions, metrics: datapoint_obj_2.metrics } }

        it 'builds a tranquilizer for each datasource and reuse them and rebuild when the schema changes' do
          expect(Druid::Writer::Tranquilizer::Base).to receive(:new).exactly(4).times.and_call_original
          expect(subject).to receive(:most_recent_segment_metadata).with(datasource_a)
          expect(subject).to receive(:send).exactly(n * 6).times

          n.times { subject.write_point(datasource_a, datapoint_1) }
          n.times { subject.write_point(datasource_a, datapoint_2) }
          n.times { subject.write_point(datasource_a, datapoint_1) }

          n.times { subject.write_point(datasource_b, datapoint_1) }
          n.times { subject.write_point(datasource_b, datapoint_2) }
          n.times { subject.write_point(datasource_b, datapoint_1) }

          expect(subject.tranquilizers.size).to eq 2
        end
      end
    end
  end
end
