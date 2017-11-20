#! /usr/bin/env perl
use strict;
use warnings;
use Test::More;
use File::Spec::Functions 'catfile';
use Log::Any '$log';
use Log::Any::Adapter 'TAP';

use_ok( 'Data::RecordExtractor::Field' ) or BAIL_OUT;
use_ok( 'Data::RecordExtractor' ) or BAIL_OUT;

subtest basic => sub {
	my $ex= new_ok( 'Data::RecordExtractor', [
			input => open_data('AddressAuxData.xlsx'),
			fields => [
				{ name => 'address' },
				{ name => 'city' },
				{ name => 'state' },
				{ name => 'zip' },
			],
			logger => $log,
		], 'Record extractor' );
	isa_ok( $ex->decoder, 'Data::RecordExtractor::Decoder::XLSX', 'detected format' );
	isa_ok( $ex->fields->[3], 'Data::RecordExtractor::Field', 'coerced fields list' );
	ok( $ex->find_table, 'found table' );
	is_deeply( $ex->col_map, [@{ $ex->fields }[99,0,1,2,3]], 'col map' );
	is_deeply( $ex->field_map, { address => 1, city => 2, state => 3, zip => 4 }, 'field map' );
	ok( my $i= $ex->iterator, 'iterator' );
	is_deeply( $i->(), { address => '123 Long St', city => 'Somewhere', state => 'OH', zip => '45678' }, 'first row' );
};

done_testing;

sub open_data {
	my $name= shift;
	my $t_dir= __FILE__;
	$t_dir =~ s,[^\/]+$,,;
	$name= catfile($t_dir, 'data', $name);
	open(my $fh, "<:raw", $name) or die "open($name): $!";
	return $fh;
}