package Data::TableReader::Decoder::XLS;

use Moo 2;
use Carp;
extends 'Data::TableReader::Decoder::Spreadsheet';

our @xls_probe_modules= qw( Spreadsheet::ParseExcel );
our $default_xls_module;
sub default_xls_module {
	$default_xls_module ||= do {
		eval "require $_" && return $_ for @xls_probe_modules;
		croak "No XLS parser available; install one of: ".join(', ', @xls_probe_modules);
	};
}

# ABSTRACT: Access sheets/rows of a Microsoft Excel '97 workbook

=head1 DESCRIPTION

See L<Data::TableReader::Decoder::Spreadsheet>.
This subclass simply parses the input using an instance of L<Spreadsheet::ParseExcel>.

=cut

sub _build_workbook {
	my $self= shift;
	
	my $wbook;
	my $f= $self->file_handle;
	if (ref $f and ref($f)->can('worksheets')) {
		$wbook= $f;
	} else {
		$wbook= $self->default_xls_module->new->parse($f);
	}
	defined $wbook or croak "Can't parse file '".$self->file_name."'";
	return $wbook;
}

1;
