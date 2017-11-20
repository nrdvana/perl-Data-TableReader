#! /usr/bin/env perl
use strict;
use warnings;
use Test::More;
use File::Spec::Functions 'catfile';

subtest XLS => sub {
	use_ok( 'Data::RecordExtractor::Decoder::XLS' ) or die;
	my $xls= new_ok( 'Data::RecordExtractor::Decoder::XLS',
		[ file_name => '', file_handle => open_data('AddressAuxData.xls'), logger => sub {} ],
		'XLS decoder' );
	run_test($xls);
};

subtest XLSX => sub {
	use_ok( 'Data::RecordExtractor::Decoder::XLSX' ) or BAIL_OUT;
	my $xlsx= new_ok( 'Data::RecordExtractor::Decoder::XLSX',
		[ file_name => '', file_handle => open_data('AddressAuxData.xlsx'), logger => sub {} ],
		'XLSX decoder' );
	run_test($xlsx);
};

done_testing;

# Both worksheets have the same data, so just repeat the tests
sub run_test {
	my $decoder= shift;

	ok( my $iter= $decoder->iterator, 'got iterator' );

	is_deeply( $iter->(), [ 'Name', 'Address', 'City', 'State', 'Zip' ], 'row 1' );
	is_deeply( $iter->(), [ 'Someone', '123 Long St', 'Somewhere', 'OH', 45678 ], 'row 2' );
	ok( my $pos= $iter->tell, 'tell position' );
	is_deeply( $iter->(), [ ('') x 5 ], 'row 3 blank' );
	is_deeply( $iter->(), [ 'Another', '01 Main St', 'Elsewhere', 'OH', 45678 ], 'row 4' );
	is_deeply( $iter->(), undef, 'no row 5' );

	ok( $iter->next_dataset, 'next dataset (worksheet)' );
	is_deeply( $iter->(), [ 'Zip Codes', '', '', '', 'Cities', '', '', '', 'State Postal Codes', '', '' ], 'sheet 2, first row' );

	ok( $iter->seek($pos), 'seek back to previous sheet' );
	is_deeply( $iter->(), [ ('') x 5 ], 'row 3 blank' );
	is_deeply( $iter->(), [ 'Another', '01 Main St', 'Elsewhere', 'OH', 45678 ], 'row 4' );
	is_deeply( $iter->(), undef, 'no row 5' );
}

sub open_data {
	my $name= shift;
	my $t_dir= __FILE__;
	$t_dir =~ s,[^\/]+$,,;
	$name= catfile($t_dir, 'data', $name);
	open(my $fh, "<:raw", $name) or die "open($name): $!";
	return $fh;
}
