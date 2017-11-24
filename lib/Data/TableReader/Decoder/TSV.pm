package Data::TableReader::Decoder::TSV;

use Moo 2;
use Try::Tiny;
use Carp;
use Log::Any '$log';

# ABSTRACT: Access rows of a tab-delimited text file

=head1 DESCRIPTION

This module is a simple subclass of L<Data::TableReader::Decoder::CSV>
which supplies these defaults for the parser:

  parser => { 
    binary => 1,
    allow_loose_quotes => 1,
    sep_char => "\t",
    escape_char => undef,
    auto_diag => 2,
  }

=cut

extends 'Data::TableReader::Decoder::CSV';

sub _build_parser {
	my $args= shift->_parser_args || {};
	Data::TableReader::Decoder::CSV->_csv_class->new({
		binary => 1,
		allow_loose_quotes => 1,
		auto_diag => 2,
		sep_char => "\t",
		escape_char => undef,
		%$args,
	});
}

1;
