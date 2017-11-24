#! /usr/bin/env perl
use strict;
use warnings;
use Test::More;
use File::Spec::Functions 'catfile';
use Log::Any '$log';
use Log::Any::Adapter 'TAP';

use_ok( 'Data::TableReader::Field' ) or BAIL_OUT;
use_ok( 'Data::TableReader' ) or BAIL_OUT;

# Find fields in the exact order they are present in the file
subtest basic => sub {
	my $ex= new_ok( 'Data::TableReader', [
			input => open_data('AddressAuxData.xlsx'),
			fields => [
				{ name => 'name' },
				{ name => 'address' },
				{ name => 'city' },
				{ name => 'state' },
				{ name => 'zip' },
			],
			log => $log,
		], 'TableReader' );
	isa_ok( $ex->decoder, 'Data::TableReader::Decoder::XLSX', 'detected format' );
	isa_ok( $ex->fields->[3], 'Data::TableReader::Field', 'coerced fields list' );
	ok( $ex->find_table, 'found table' );
	is_deeply( $ex->col_map, $ex->fields, 'col map' );
	is_deeply( $ex->field_map, { name => 0, address => 1, city => 2, state => 3, zip => 4 }, 'field map' );
	ok( my $i= $ex->iterator, 'iterator' );
	is_deeply( $i->(), { name => 'Someone', address => '123 Long St', city => 'Somewhere', state => 'OH', zip => '45678' }, 'first row' );
	is_deeply( $i->(), { name => 'Another', address => '01 Main St', city => 'Elsewhere', state => 'OH', zip => '45678' }, 'third row' );
	is( $i->(), undef, 'eof' );
};

subtest find_on_second_sheet => sub {
	my $ex= new_ok( 'Data::TableReader', [
			input => open_data('AddressAuxData.xlsx'),
			fields => [
				{ name => 'postcode' },
				{ name => 'country' },
				{ name => 'state' },
			],
			log => $log,
		], 'TableReader' );
	ok( $ex->find_table, 'found table' );
	ok( my $i= $ex->iterator, 'iterator' );
	is_deeply( $i->(), { state => 'Alberta', postcode => 'AB', country => 'CA' }, 'row 1' );
	is_deeply( $i->(), { state => 'Alaska',  postcode => 'AK', country => 'US' }, 'row 2' );
	is_deeply( $i->(), { state => 'Alabama', postcode => 'AL', country => 'US' }, 'row 3' );
	is_deeply( $i->(), { state => 'Arkansas',postcode => 'AR', country => 'US' }, 'row 4' );
	is_deeply( $i->(), { state => 'American Samoa', postcode => 'AS', country => 'US' }, 'row 5' );
	is( $i->(), undef, 'eof' );
};

subtest find_required => sub {
	open(my $csv, '<', \"q,w,e,r,t,y\nq,w,e,r,t,a,s,d\n") or die;
	my $ex= new_ok( 'Data::TableReader', [
			input => $csv,
			fields => [
				{ name => 'q', required => 1 },
				{ name => 'w', required => 1 },
				{ name => 'a', required => 0 },
				{ name => 'b', required => 0 },
				{ name => 'y', required => 0 },
				{ name => 's', required => 1 },
			],
			log => $log,
		], 'TableReader' );
	ok( $ex->find_table, 'found table' );
	is_deeply( $ex->field_map, { q => 0, w => 1, a => 5, s => 6 }, 'field_map' );
	is_deeply( $ex->iterator->all(), [], 'immediate eof' );
};

subtest multiline_header => sub {
	open(my $csv, '<', \"a,b,c\nd,e,f\ng,b,c\nA,B,C") or die;
	my $ex= new_ok( 'Data::TableReader', [
			input => $csv,
			fields => [
				{ name => 'a', header => "d g" },
				{ name => 'b', header => 'b' },
				{ name => 'c', header => qr/^f\nc$/ },
			],
			log => $log,
		], 'TableReader' );
	is( $ex->header_row_combine, 2, 'header_row_combine' );
	ok( $ex->find_table, 'found table' );
	is_deeply( $ex->field_map, { a => 0, b => 1, c => 2 }, 'field_map' );
	is_deeply( $ex->iterator->all(), [{a=>'A',b=>'B',c=>'C'}], 'found row' );
};

subtest multi_column => sub {
	open(my $csv, '<', \"a,b,a,c,a,d\n1,2,3,4,5,6\n") or die;
	my $ex= new_ok( 'Data::TableReader', [
			input => $csv,
			fields => [
				{ name => 'a', header => qr/a|c/, array => 1 },
				{ name => 'd' },
			],
			log => $log,
		], 'TableReader' );
	ok( $ex->find_table, 'found table' );
	is_deeply( $ex->field_map, { a => [0,2,3,4], d => 5 }, 'field_map' );
	is_deeply( $ex->iterator->all(), [{a => [1,3,4,5], d => 6}], 'rows' );
};

subtest array_at_end => sub {
	open(my $csv, '<', \"a,b,c,,,,,\n1,2,3,4,5,6,7,8,9\n1,2,3,4,5,6,7,8,9,10,11,12,13\n1,2,3,4") or die;
	my $ex= new_ok( 'Data::TableReader', [
			input => $csv,
			fields => [
				'a',
				{ name => 'c', array => 1 },
				{ name => 'c', array => 1, header => '', follows => 'c' },
			],
			log => $log,
		], 'TableReader' );
	ok( $ex->find_table, 'found table' );
	is_deeply( $ex->field_map, { a => 0, c => [2,3,4,5,6,7] }, 'field_map' );
	my $i= $ex->iterator;
	is_deeply( $i->(), { a => 1, c => [3,4,5,6,7,8] }, 'row1' );
	is_deeply( $i->(), { a => 1, c => [3,4,5,6,7,8] }, 'row1' );
	is_deeply( $i->(), { a => 1, c => [3,4,undef,undef,undef,undef] }, 'row1' );
	is( $i->(), undef, 'eof' );
};

subtest complex_follows => sub {
	open(my $csv, '<', \(
		 "name,start coords,,,,end coords,,,,\n"
		."    ,x,y,w,h,x,y,w,h\n"
		."foo,1,1,6,6,2,2,8,8\n"
	)) or die;
	my $ex= new_ok( 'Data::TableReader', [
			input => $csv,
			fields => [
				'name',
				{ name => 'start_x', header => qr/start.*\nx/ },
				{ name => 'start_y', header => 'y', follows => 'start_x' },
				{ name => 'end_x', header => qr/end.*\nx/ },
				{ name => 'end_y', header => 'y', follows => 'end_x' }
			],
			log => $log,
		], 'TableReader' );
	ok( $ex->find_table, 'found table' );
	is_deeply( $ex->field_map, { name => 0, start_x => 1, start_y => 2, end_x => 5, end_y => 6 }, 'field_map' );
	my $i= $ex->iterator;
	is_deeply( $i->(), { name => 'foo', start_x => 1, start_y => 1, end_x => 2, end_y => 2 }, 'row1' );
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
