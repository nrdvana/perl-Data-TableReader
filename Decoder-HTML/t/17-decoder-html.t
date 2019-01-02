#! /usr/bin/env perl
use strict;
use warnings;
use FindBin;
use Test::More;
use Try::Tiny;
use File::Spec::Functions 'catfile';
use Log::Any '$log';
use Log::Any::Adapter 'TAP';
use Data::TableReader::Decoder::HTML;
use Data::TableReader;
my $log_fn= sub { $log->can($_[0])->($log, $_[1]) };

my $fname= catfile( $FindBin::Bin, 'data', 'Data.html' );
my @expected_data= (
	[ # Table 1
		[ qw( Name   Address        City         State  Zip  ) ],
		[ 'Someone','123 Long St', 'Somewhere', 'OH', 45678 ],
		[ 'Another','01 Main St',  'Elsewhere', 'OH', 45678 ],
	]
);

subtest simple_iteration => \&test_simple_iteration;
sub test_simple_iteration {
	open my $fh, '<:raw', $fname or die "open: $!";
	my $dec= new_ok( 'Data::TableReader::Decoder::HTML',
		[ file_name => $fname, file_handle => $fh, _log => $log_fn ],
		'new HTML decoder'
	);
	ok( $dec->parse, 'able to parse HTML' );
	ok( my $iter= $dec->iterator, 'got iterator' );
	for (@{ $expected_data[0] }) {
		is_deeply( $iter->(), $_, $iter->position );
	}
	
	done_testing;
}

subtest seek_tell => \&test_seek_tell;
sub test_seek_tell {
	open my $fh, '<:raw', $fname or die "open: $!";
	my $dec= new_ok( 'Data::TableReader::Decoder::HTML',
		[ file_name => $fname, file_handle => $fh, _log => $log_fn ],
		'new HTML decoder'
	);
	ok( $dec->parse, 'able to parse HTML' );
	ok( my $iter= $dec->iterator, 'got iterator' );
	my $pos= $iter->tell;
	is( $iter->progress, 0, 'progress=0' );
	is_deeply( $iter->(), $expected_data[0][0], 'correct first row' );
	$iter->seek($pos);
	is( $iter->progress, 0, 'progress=0 again' );
	is_deeply( $iter->(), $expected_data[0][0], 'correct first row' );
	
	done_testing;
}

done_testing;