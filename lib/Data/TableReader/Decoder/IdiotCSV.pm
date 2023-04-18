package Data::TableReader::Decoder::IdiotCSV;
use Moo 2;
use Try::Tiny;
use Carp;
use Log::Any '$log';

extends 'Data::TableReader::Decoder::CSV';

# ABSTRACT: Access rows of a badly formatted comma-delimited text file
# VERSION

=head1 DESCRIPTION

This decoder is like L<Data::TableReader::Decoder::CSV|::Decoder::CSV>, but can additionally
parse the garbage resulting from those special people who write "CSV Export" code
that looks like

  print join(",", map qq{"$_"}, @record)."\n";

(or rather the equivalent code in Visual Basic or PHP which is what they're
probably using)  regardless of their data containing quote characters or newlines,
resulting in garbage like:

  "First Name","Last Name","email"
  "Joseph "Joe","Smith",""Smith, Joe" <jsmith@example.com>"

This can actually be processed by (recent versions of) the L<Text::CSV> module
with the following configuration:

  {
    binary => 1,
    allow_loose_quotes => 1,
    allow_whitespace => 1,
    escape_char => undef,
  }

And so this module is simply a subclass of L<Data::TableReader::Decoder::CSV>
which provides those defaults to the parser.

How does the parsing work though?  Well, some guesswork and patterns.  It's not super
reliable, and you should always complain loudly to whoever generated that data,
unless they're a much larger company than you and would never listen, or went
out of business a while back, in which case you can justify using this module
in production.

=cut

sub _build_parser {
	my $args= shift->_parser_args || {};
	Data::TableReader::Decoder::CSV->default_csv_module->new({
		binary => 1,
		allow_loose_quotes => 1,
		allow_whitespace => 1,
		auto_diag => 1,
		escape_char => undef,
		%$args,
	});
}

1;
