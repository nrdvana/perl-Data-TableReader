package Data::TableReader::Decoder::XLSX;

use Moo 2;
use Carp;
use Try::Tiny;
extends 'Data::TableReader::Decoder::Spreadsheet';

our @xlsx_probe_modules= qw( Spreadsheet::ParseXLSX Spreadsheet::XLSX );
our $default_xlsx_module;
sub default_xlsx_module {
	$default_xlsx_module ||= do {
		eval "require $_" && return $_ for @xlsx_probe_modules;
		croak "No CSV parser available; install one of: ".join(', ', @xlsx_probe_modules);
	};
}

# ABSTRACT: Access sheets/rows of a modern XML-based Microsoft Excel spreadsheet

=head1 DESCRIPTION

See L<Data::TableReader::Decoder::Spreadsheet>.
This subclass simply parses the input using an instance of L<Spreadsheet::ParseXLSX>.

=cut

sub _build_workbook {
	my $self= shift;
	
	my $wbook;
	my $f= $self->file_handle;
	if (ref $f and ref($f)->can('worksheets')) {
		$wbook= $f;
	} else {
		my $class= $self->default_xlsx_module;
		# Spreadsheet::XLSX has an incompatible constructor
		if ($class->isa('Spreadsheet::XLSX')) {
			$wbook= $class->new($f);
		} else {
			$wbook= $class->new->parse($f);
		}
	}
	defined $wbook or croak "Can't parse file '".$self->file_name."'";
	return $wbook;
}

1;