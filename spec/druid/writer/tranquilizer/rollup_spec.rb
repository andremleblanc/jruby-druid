require 'spec_helper'

describe Druid::Writer::Tranquilizer::Rollup do
  subject { Druid::Writer::Tranquilizer::Rollup }
  let(:args) { { config: config, dimensions: dimensions.keys, metrics: metrics.keys } }
  let(:config) { Druid::Configuration.new() }
  let(:dimensions) { { 'manufacturer' => 'ACME' } }
  let(:metrics) { { 'anvils' => 1 } }

  describe '.build' do
    it 'builds a DruidRollup' do
      expect(subject.build(args)).to be_a com.metamx.tranquility.druid.DruidRollup
    end
  end
end
