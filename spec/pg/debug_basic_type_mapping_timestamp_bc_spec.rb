# -*- rspec -*-
# encoding: utf-8

require_relative '../helpers'

require 'pg'
require 'time'

def restore_type(types)
	[0, 1].each do |format|
		[types].flatten.each do |type|
			PG::BasicTypeRegistry.alias_type(format, "restore_#{type}", type)
		end
	end
	yield
ensure
	[0, 1].each do |format|
		[types].flatten.each do |type|
			PG::BasicTypeRegistry.alias_type(format, type, "restore_#{type}")
		end
	end
end

describe 'Basic type mapping' do

	describe PG::BasicTypeMapForResults do
		let!(:basic_type_mapping) do
			PG::BasicTypeMapForResults.new @conn
		end

		#
		# Decoding Examples text+binary format converters
		#

		describe "connection wide type mapping" do
			before :each do
				@conn.type_map_for_results = basic_type_mapping
			end

			# Debug format: 1 => ng case, 0 => ok case
			[1].each do |format|
			# [1, 0].each do |format|
				it "should convert format #{format} timestamps per TimestampUtc" do
					restore_type("timestamp") do
						PG::BasicTypeRegistry.register_type 0, 'timestamp', nil, PG::TextDecoder::TimestampUtc
						@conn.type_map_for_results = PG::BasicTypeMapForResults.new(@conn)
						res = @conn.exec_params( "SELECT CAST('4714-11-24 23:58:59.1231-03 BC' AS TIMESTAMP WITHOUT TIME ZONE)", [], format )

						puts("[DEBUG] format: #{format}")
						debug_cmd = %q[echo "SELECT CAST('4714-11-24 23:58:59.1231-03 BC' AS TIMESTAMP WITHOUT TIME ZONE);" | psql --host=localhost --port=54321 test]
						debug_out = `#{debug_cmd}`
						puts("[DEBUG] debug_out: #{debug_out}")
						# => 4714-11-24 23:58:59.1231 BC

						# res.getvalue(0,2) is different on architecture: i686 and armv7hl.
						# On i686 and armv7hl (error case)
						# res.getvalue(0,2) => 1956-11-26 05:04:03 UTC
						#   format 1: => 1956-11-26 05:04:03 UTC => ng
						#   format 0: => -4713-11-24 23:58:59 UTC => ok
						# On others: x86_64, ppc64le, aarch64, s390x (ok case)
						# format 1, 0 common: res.getvalue(0,2) => -4713-11-24 23:58:59 UTC
						puts("[DEBUG] value: #{res.getvalue(0,0)}")
						puts("[DEBUG] value iso8601(3): #{res.getvalue(0,0).iso8601(3)}")
						# Time.utc is correct.
						puts("[DEBUG] Time.utc: #{Time.utc(-4713, 11, 24, 23, 58, 59.1231)}")
						puts("[DEBUG] Time.utc iso8601(3): #{Time.utc(-4713, 11, 24, 23, 58, 59.1231).iso8601(3)}")
						expect( res.getvalue(0,0).iso8601(3) ).to eq( Time.utc(-4713, 11, 24, 23, 58, 59.1231).iso8601(3) )
					end
				end
			end

			[1].each do |format|
				it "should convert format #{format} timestamps per TimestampUtcToLocal" do
					restore_type("timestamp") do
						PG::BasicTypeRegistry.register_type 0, 'timestamp', nil, PG::TextDecoder::TimestampUtcToLocal
						PG::BasicTypeRegistry.register_type 1, 'timestamp', nil, PG::BinaryDecoder::TimestampUtcToLocal
						@conn.type_map_for_results = PG::BasicTypeMapForResults.new(@conn)
						res = @conn.exec_params( "SELECT CAST('4714-11-24 23:58:59.1231-03 BC' AS TIMESTAMP WITHOUT TIME ZONE)", [], format )
						expect( res.getvalue(0,0).iso8601(3) ).to eq( Time.utc(-4713, 11, 24, 23, 58, 59.1231).getlocal.iso8601(3) )
					end
				end
			end

			[1].each do |format|
				it "should convert format #{format} timestamps per TimestampLocal" do
					restore_type("timestamp") do
						PG::BasicTypeRegistry.register_type 0, 'timestamp', nil, PG::TextDecoder::TimestampLocal
						PG::BasicTypeRegistry.register_type 1, 'timestamp', nil, PG::BinaryDecoder::TimestampLocal
						@conn.type_map_for_results = PG::BasicTypeMapForResults.new(@conn)
						res = @conn.exec_params( "SELECT CAST('4714-11-24 23:58:59.1231-03 BC' AS TIMESTAMP WITHOUT TIME ZONE)", [], format )
						expect( res.getvalue(0,0).iso8601(3) ).to eq( Time.new(-4713, 11, 24, 23, 58, 59.1231).iso8601(3) )
					end
				end
			end

			[1].each do |format|
				it "should convert format #{format} timestamps with time zone" do
					res = @conn.exec_params( "SELECT CAST('4714-11-24 23:58:59.1231-03 BC' AS TIMESTAMP WITH TIME ZONE)", [], format )
					expect( res.getvalue(0,0) ).to be_within(1e-3).of( Time.new(-4713, 11, 24, 23, 58, 59.1231, "-03:00").getlocal )
				end
			end

		end
	end
end
