#! /usr/bin/env perl
use strict;
use warnings;
use Test::More;
use File::Spec::Functions 'catfile';
use Log::Any '$log';
use Log::Any::Adapter 'TAP';

use_ok( 'Data::RecordExtractor::Field' ) or BAIL_OUT;
use_ok( 'Data::RecordExtractor' ) or BAIL_OUT;

# Find fields in the exact order they are present in the file
subtest basic => sub {
	my $ex= new_ok( 'Data::RecordExtractor', [
			input => open_data('AddressAuxData.xlsx'),
			fields => [
				{ name => 'name' },
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
	is_deeply( $ex->col_map, $ex->fields, 'col map' );
	is_deeply( $ex->field_map, { name => 0, address => 1, city => 2, state => 3, zip => 4 }, 'field map' );
	ok( my $i= $ex->iterator, 'iterator' );
	is_deeply( $i->(), { name => 'Someone', address => '123 Long St', city => 'Somewhere', state => 'OH', zip => '45678' }, 'first row' );
	is_deeply( $i->(), { name => 'Another', address => '01 Main St', city => 'Elsewhere', state => 'OH', zip => '45678' }, 'third row' );
	is( $i->(), undef, 'eof' );
};

subtest find_on_second_sheet => sub {
	my $ex= new_ok( 'Data::RecordExtractor', [
			input => open_data('AddressAuxData.xlsx'),
			fields => [
				{ name => 'postcode', required => 1 },
				{ name => 'country', required => 1 },
				{ name => 'state', required => 1 },
			],
			logger => $log,
		], 'Record extractor' );
	ok( $ex->find_table, 'found table' );
	ok( my $i= $ex->iterator, 'iterator' );
	is_deeply( $i->(), { state => 'Alberta', postcode => 'AB', country => 'CA' }, 'row 1' );
	is_deeply( $i->(), { state => 'Alaska',  postcode => 'AK', country => 'US' }, 'row 2' );
	is_deeply( $i->(), { state => 'Alabama', postcode => 'AL', country => 'US' }, 'row 3' );
	is_deeply( $i->(), { state => 'Arkansas',postcode => 'AR', country => 'US' }, 'row 4' );
	is_deeply( $i->(), { state => 'American Samoa', postcode => 'AS', country => 'US' }, 'row 5' );
	is( $i->(), undef, 'eof' );
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
