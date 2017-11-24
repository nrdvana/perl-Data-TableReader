package Data::TableReader::Decoder::Spreadsheet;

use Moo 2;
use Carp 'croak';
use IO::Handle;

extends 'Data::TableReader::Decoder';

=head1 DESCRIPTION

This is a base class for any file format that exposes a spreadsheet API
compatible with L<Spreadsheet::ParseExcel>.

=head1 ATTRIBUTES

See attributes from parent class: L<Data::TableReader::Decoder>.

=head2 C<workbook>

This is an instance of L<Spreadsheet::ParseExcel>, L<Spreadsheet::ParseXLSX>,
or L<Spreadsheet::XLSX> (which all happen have the same API).  Subclasses can
lazy-build this from the C<file_handle>.

=cut

has workbook => ( is => 'lazy' );

=head2 C<sheet>

This is either a sheet name, a regex for matching a sheet name, or a parser's
worksheet object.  It is also optional; if not set, all sheets will be iterated.

=cut

has sheet => ( is => 'ro' );

# Arrayref of all sheets we can search
has _sheets => ( is => 'lazy' );

sub _build__sheets {
	my $self= shift;

	# If we have ->sheet and it is a worksheet object, then no need to do anything else
	if ($self->sheet && ref($self->sheet) && ref($self->sheet)->can('get_cell')) {
		return [ $self->sheet ];
	}

	# Else we need to scan sheets from the excel file.  Make sure we have the file
	my @sheets= $self->workbook->worksheets;
	@sheets or croak "No worksheets in file?";
	if (defined $self->sheet) {
		if (ref($self->sheet) eq 'Regexp') {
			@sheets= grep { $_->get_name =~ $self->sheet } @sheets;
		} elsif (ref($self->sheet) eq 'CODE') {
			@sheets= grep { $self->sheet->($_) } @sheets;
		} elsif (!ref $self->sheet) {
			@sheets= grep { $_->get_name eq $self->sheet } @sheets;
		} else {
			croak "Unknown type of sheet specification: ".$self->sheet;
		}
	}

	return \@sheets;
}

sub _build_iterator {
	my $self= shift;
	my $sheets= $self->_sheets;
	my $sheet= $sheets->[0];
	my ($colmin, $colmax)= $sheet? $sheet->col_range() : (0,-1);
	my ($rowmin, $rowmax)= $sheet? $sheet->row_range() : (0,-1);
	my $row= $rowmin-1;
	Data::TableReader::Decoder::Spreadsheet::_Iterator->new(
		sub {
			my $slice= shift;
			return undef unless $row < $rowmax;
			++$row;
			my $x;
			if ($slice) {
				return [ map {
					$x= ($x= $sheet->get_cell($row, $_)) && $x->value;
					defined $x? $x : ''
				} @$slice ];
			} else {
				return [ map {
					$x= ($x= $sheet->get_cell($row, $_)) && $x->value;
					defined $x? $x : ''
				} 0 .. $colmax ];
			}
		},
		{
			sheets => $sheets,
			sheet_idx => 0,
			sheet_ref => \$sheet,
			row_ref => \$row,
			colmax_ref => \$colmax,
			rowmax_ref => \$rowmax,
			origin => [ $sheet, $row ],
		}
	);
}

{ package # Hide from CPAN
	Data::TableReader::Decoder::Spreadsheet::_Iterator;
	use strict;
	use warnings;
	use Carp;
	use parent 'Data::TableReader::Iterator';

	sub position {
		my $f= shift->_fields;
		'row '.${ $f->{row_ref} };
	}
   
	sub progress {
		my $f= shift->_fields;
		return ${ $f->{row_ref} } / (${ $f->{rowmax_ref} } || 1);
	}

	sub tell {
		my $f= shift->_fields;
		return [ $f->{sheet_idx}, ${$f->{row_ref}} ];
	}

	sub seek {
		my ($self, $to)= @_;
		my $f= $self->_fields;
		$to ||= $f->{origin};
		my ($sheet_idx, $row)= @$to;
		my $sheet= $f->{sheets}[$sheet_idx];
		my ($colmin, $colmax)= $sheet? $sheet->col_range() : (0,-1);
		my ($rowmin, $rowmax)= $sheet? $sheet->row_range() : (0,-1);
		$row= $rowmin-1 unless defined $row;
		$f->{sheet_idx}= $sheet_idx;
		${$f->{sheet_ref}}= $sheet;
		${$f->{row_ref}}= $row;
		${$f->{colmax_ref}}= $colmax;
		${$f->{rowmax_ref}}= $rowmax;
		1;
	}
	
	sub next_dataset {
		my $self= shift;
		my $f= $self->_fields;
		return defined $f->{sheets}[ $f->{sheet_idx}+1 ]
			&& $self->seek([ $f->{sheet_idx}+1 ]);
	}
}

1;

