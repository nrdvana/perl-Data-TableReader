package Data::TableReader::Decoder::XLS;
use Moo 2;
use Carp;
extends 'Data::TableReader::Decoder::Spreadsheet';

our @xls_probe_modules= ( [ 'Spreadsheet::ParseExcel', '0.66' ] );
our $default_xls_module;
sub default_xls_module {
	$default_xls_module ||=
		Data::TableReader::Decoder::_first_sufficient_module('XLS parser', \@xls_probe_modules);
}

# ABSTRACT: Access sheets/rows of a Microsoft Excel '97 workbook
# VERSION

=head1 DESCRIPTION

See L<Data::TableReader::Decoder::Spreadsheet>.
This subclass simply parses the input using an instance of L<Spreadsheet::ParseExcel>.

=head1 CLASS METHODS

=head2 default_xls_module

Initializes C<@Data::TableReader::Decoder::XLS::default_xls_module> to the first
available module in the list of C<@Data::TableReader::Decoder::XLS::xls_probe_modules>
and returns the cached value every time afterward.

Those variables can be modified as needed, if you have other XLS modules available.

=cut

sub _build_workbook {
	my $self= shift;
	
	my $wbook;
	my $f= $self->file_handle;
	if (ref $f and ref($f)->can('worksheets')) {
		$wbook= $f;
	} else {
		$wbook= $self->default_xls_module->new->parse($f, $self->xls_formatter);
	}
	defined $wbook or croak "Can't parse file '".$self->file_name."'";
	return $wbook;
}

1;
