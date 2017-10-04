#! /usr/bin/env perl
use strict;
use warnings;
use Test::More;
use File::Spec::Functions 'catfile';

use_ok( 'Data::RecordExtractor::Field' ) or die;
use_ok( 'Data::RecordExtractor' ) or die;

subtest basic => sub {
	my $ex= new_ok( 'Data::RecordExtractor', [
			input => open_data('AddressAuxData.xlsx'),
			fields => [
				{ name => 'address' },
				{ name => 'city' },
				{ name => 'state' },
				{ name => 'zip' },
			]
		], 'Record extractor' );
	isa_ok( $ex->decoder, 'Data::RecordExtractor::Decoder::XLSX', 'detected format' );
	isa_ok( $ex->fields->[3], 'Data::RecordExtractor::Field', 'coerced fields list' );
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