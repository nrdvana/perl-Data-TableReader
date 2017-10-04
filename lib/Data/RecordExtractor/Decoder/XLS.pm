package Data::RecordExtractor::Decoder::XLS;
use Moo 2;
use Carp;
extends 'Data::RecordExtractor::Decoder::Spreadsheet';

=head1 DESCRIPTION

See L<Data::RecordExtractor::Decoder::Spreadsheet>.
This subclass simply initializes the parser with an instance of
L<Spreadsheet::ParseExcel>.

=cut

sub _build_workbook {
	my $self= shift;
	
	my $wbook;
	my $f= $self->file_handle;
	if (ref $f and ref($f)->can('worksheets')) {
		$wbook= $f;
	} else {
		require Spreadsheet::ParseExcel;
		$wbook= Spreadsheet::ParseExcel->new->parse($f);
	}
	defined $wbook or croak "Can't parse file '".$self->file_name."'";
	return $wbook;
}

1;