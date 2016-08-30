require 'spec_helper'

describe Druid::Writer::Tranquilizer::Base do
  subject { Druid::Writer::Tranquilizer::Base.new(params) }
  let(:config) { Druid::Configuration.new }
  let(:datapoint) { Druid::Writer::Tranquilizer::Datapoint.new(datapoint_params) }
  let(:datapoint_params) { { dimensions: dimensions, metrics: metrics } }
  let(:datasource_name) { 'baz' }
  let(:dimensions) { { 'manufacturer' => 'ACME' } }
  let(:metrics) { { 'anvils' => 1 } }
  let(:params) {{ config: config, datasource: datasource_name, dimensions: datapoint.dimensions, metrics: datapoint.metrics }}

  describe '.new' do
    context 'with no params' do
      subject { Druid::Writer::Tranquilizer::Base.new() }
      it { expect{subject}.to raise_error ArgumentError }
    end

    context 'with config' do
      it { expect{subject}.not_to raise_error }
    end
  end

  describe '#dimensions' do
    context 'first call' do
      it 'combines the current schema with the dimensions being passed in' do

      end
    end

    context 'second call' do
      it 'uses the memoized value' do

      end
    end
  end

  describe '#metric_keys' do

  end

  describe '#safe_send' do
    xit 'needs ZK; returns a future' do
      pending('Needs ZK to run')
      expect(subject.safe_send(datapoint)).to be_a Java::ComTwitterUtil::Promise::Chained
    end
  end

  describe '#start' do
    it 'starts curator and service' do
      expect(subject.curator).to receive(:start)
      expect(subject.service).to receive(:start)
      subject.start
    end
  end

  describe '#stop' do
    it 'starts curator and service' do
      expect(subject.service).to receive(:flush)
      expect(subject.service).to receive(:stop)
      expect(subject.curator).to receive(:close)
      subject.stop
    end
  end
end
