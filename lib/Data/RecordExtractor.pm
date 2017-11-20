package Data::RecordExtractor;

use Moo 2;
use Try::Tiny;
use URI;
use Carp;
use Log::Any '$log';
use List::Util 'max';
use Module::Runtime 'require_module';
use Data::RecordExtractor::Field;

# ABSTRACT: Extract records from "dirty" tabular data sources

=head1 SYNOPSIS

  my $ex= Data::RecordExtractor->new(
    # path or file handle
    # let it auto-detect the format (but can override that if we need)
    input => 'path/to/file.csv',
    
    # We want these fields to exist in the file (identified by headers)
    fields => [
      { name => 'address', header => qw/street|address/i },
      'city',
      'state',
      # can validate with Type::Tiny classes
      { name => 'zip', header => qw/zip\b|postal/i, type => US_Zipcode },
    ],
    
    # could do this after extraction, but this fixes it before the type validation
    filters => [
       # they keep losing the leading zeroes on our zip codes. grr.
       sub {
         $_[0]{zip} =~ s/^(\d+)(-(\d+))?$/sprintf("%05d-%04d", $1, $3||0)/e;
         return $_[0];
       },
    ],
    
    # Our data provider is horrible; just ignore any nonsense we encounter
    on_blank_rows => 'next',
    on_validation_fail => 'next',
    
    # Capture warnings and show to user who uploaded file
    logger => \(my @messages)
  );
  
  my $records= $ex->iterator->all;
  ...
  $http_response->body( encode_json({ messages => \@messages }) );

=head1 DESCRIPTION

This module is designed to be a useful for anyone who needs to take "loose"
or "dirty" tabular data sources (such as Excel, CSV, TSV, or HTML) which may
have been edited by non-technical humans and extract the data into sanitized
records, while also verifying that the data file contains roughly the schema
you were expecting.  It is primarily intended for making automated imports
of data from non-automated or unstable sources, and providing human-readable
feedback about the validity of the data file.

=head1 ATTRIBUTES

=head2 input

This can be a file name or L<Path::Class> instance or file handle.  If a file
handle, it must be seekable in order to auto-detect the file format, I<or> you
may specify the decoder directly to avoid auto-detection.

=head2 decoder

This is either an instance of L<Data::RecordExtractor::Decoder>, or a class name,
or a partial class name to be appended as C<"Data::RecordExtractor::Decoder::$name">
or an arrayref or hashref of arguments to build the decoder.

Examples:

  'CSV'
  # becomes Data::RecordExtractor::Decoder::CSV->new()
  
  [ 'CSV', sep_char => "|" ]
  # becomes Data::RecordExtractor::Decoder::CSV->new(sep_char => "|")
  
  { CLASS => 'CSV', sep_char => "|" }
  # becomes Data::RecordExtractor::Decoder::CSV->new({ sep_char => "|" })

=head2 fields

An arrayref of L<Data::RecordExtractor::Field> objects (or hashrefs to
construct them with) which this module should search for within the tables
(worksheets etc.) of L</input>.

=head2 record_class

Default is the special value 'HASH' for un-blessed hashref records.
The special value 'ARRAY' will result in arrayrefs with fields in the same
order they were specified in the L</fields> specification.
Setting it to anything else will return records created with
C<< $record_class->new(\%fields); >>

=head2 filters

List of filters which should be applied to the data records after they have
been pulled from the decoder but before they have been type-checked or passed
to the record constructor (if any).  Each element of the list should be a
coderef which receives a hashref and returns it, possibly modified.

=head2 static_field_order

Boolean, whether the L</fields> must be found in columns in the exact order
that they were specified.  Default is false.

=head2 header_row_at

Row number, or range of row numbers where the header must be found.
(All row numbers in this module are 1-based, to match end-user expectations.)
The default is C<[1,10]> to limit header scanning to the first 10 rows.
As a special case, if you are reading a source which lacks headers and you
trust the source to deliver the columns in the right order, you can set this
to undef if you also set C<< static_field_order => 1 >>.

=head2 on_unknown_columns

  on_unknown_columns => 'use'  # warn, and then use the table
  on_unknown_columns => 'next' # warn, and then look for another table which matches
  on_unknown_columns => 'die'  # fatal error
  on_unknown_columns => sub {
    my ($extractor, $col_headers)= @_;
    ...;
    return $opt; # one of the above values
  }

This determines handling for columns that aren't associated with any field.
The "required" columns must all be found before it considers this setting, but once it has
found everything it needs to make this a candidate, you might or might not care about the
leftover columns.

=over

=item C<'use'>  (default)

You don't care if there are extra columns, just log warnings about them and proceed extracting
from this table.

=item C<'next'>

Extra columns mean that you didn't find the table you wanted.  Log the near-miss, and
keep searching additional rows or additional tables.

=item C<'die'>

This header is probably what you want, but you consider extra columns to be an error
condition.  Logs the details and calls C<croak>.

=item C<sub {}>

You can add your own logic to handle this.  Inspect the headers however you like, and then
return one of the above values.

=back

If you want to get cleaner default log messages, i.e. to show to users, see L</logger>.

=head2 on_blank_rows

  on_blank_rows => 'next' # warn, and then skip the row(s)
  on_blank_rows => 'last' # warn, and stop iterating the table
  on_blank_rows => 'die'  # fatal error
  on_blank_rows => 'use'  # actually try to return the blank rows as records
  on_blank_rows => sub {
    my ($extractor, $first_blank_rownum, $last_blank_rownum)= @_;
    ...;
    return $opt; # one of the above values
  }

This determines what happens when you've found the table, are extracting
records, and encounter a series of blank rows (defined as a row with no
printable characters in any field) followed by non-blank rows.
If you use the callback, it suppresses the default warning, since you can
generate your own.
If you want to get cleaner log messages, i.e. to show to users, see L</logger>.

The default is C<'next'>.

=head2 on_validation_fail

  on_validation_fail => 'next'  # warn, and then skip the record
  on_validation_fail => 'use'   # warn, and then use the record anyway
  on_validation_fail => 'die'   # fatal error
  on_validation_fail => sub {
    my ($extractor, $record, $failed_fields)= @_;
    for my $field (@$failed_fields) {
      if ($record->{ $field->name } ...) {
        ...
      }
    }
    return $opt;
  }

This determines what happens when you've found the table, are extracting
records, and one row fails its validation.  In addition to deciding an option,
the callback gives you a chance to alter the record before C<'use'>ing it.
If you use the callback, it suppresses the default warning, since you can
generate your own.
If you want to get cleaner log messages, i.e. to show to users, see L</logger>.

The default is 'die'.

=head2 logger

If undefined (the default) all log messages above 'info' will be emitted with
C<warn "$message\n">.  If set to an object, it should support an API of:

  trace,  is_trace
  debug,  is_debug
  info,   is_info
  warn,   is_warn
  error,  is_error

such as L<Log::Any> and may other perl logging modules use.  You can also
set it to a coderef such as:

  my @messages;
  sub { my ($level, $message)= @_;
    push @messages, [ $level, $message ]
      if grep { $level eq $_ } qw( info warn error );
  };

for a simple way to capture the messages without involving a logging module.
And for extra convenience, you can set it to an arrayref which will receive
any message that would otherwise have gone to 'warn' or 'error'.

=cut

has input               => ( is => 'rw', required => 1 );
has _file_handle        => ( is => 'lazy' );
has _decoder_arg        => ( is => 'rw', init_arg => 'decoder' );
has decoder             => ( is => 'lazy', init_arg => undef );
has fields              => ( is => 'rw', required => 1, coerce => \&_coerce_field_list );
sub field_list             { @{ shift->fields } }
has record_class        => ( is => 'rw', required => 1, default => sub { 'HASH' } );
has filters             => ( is => 'rw' ); # list of coderefs to apply to the data
has static_field_order  => ( is => 'rw' ); # force order of columns
has header_row_at       => ( is => 'rw', default => sub { [1,10] } ); # row of header, or range to scan
has on_unknown_columns  => ( is => 'rw', default => sub { 'use' } );
has on_blank_rows       => ( is => 'rw', default => sub { 'next' } );
has on_validation_fail  => ( is => 'rw', default => sub { 'die' } );
has logger              => ( is => 'rw' );
has col_map             => ( is => 'rw', lazy => 1, builder => 1, predicate => 1, clearer => 1 );
has field_map           => ( is => 'rw', lazy => 1, builder => 1, predicate => 1, clearer => 1 );
has iterator            => ( is => 'lazy', predicate => 1, clearer => 1 );

sub _build__file_handle {
	my $self= shift;
	my $i= $self->input;
	return $i if ref($i) && (ref($i) eq 'GLOB' or ref($i)->can('read'));
	open(my $fh, '<', $i) or croak "open($i): $!";
	return $fh;
}

sub _build_decoder {
	my $self= shift;
	my $decoder_arg= $self->_decoder_arg;
	my ($class, @args);
	if (!$decoder_arg) {
		($class, @args)= $self->detect_input_format;
	}
	elsif (!ref $decoder_arg) {
		$class= $decoder_arg;
	}
	elsif (ref $decoder_arg eq 'HASH') {
		my %tmp= %$decoder_arg;
		$class= delete $tmp{CLASS}
			or ($class)= $self->detect_input_format
			or croak "require ->{CLASS} in decoder arguments";
		@args= %tmp;
	}
	elsif (ref $decoder_arg eq 'ARRAY') {
		($class, @args)= @$decoder_arg;
	}
	elsif (ref($decoder_arg)->can('iterator')) {
		return $decoder_arg;
	}
	else {
		croak "Can't create decoder from ".ref($decoder_arg);
	}
	$class= "Data::RecordExtractor::Decoder::$class"
		unless $class =~ /::/;
	require_module($class) or croak "$class does not exist or is not installed";
	return $class->new(
		file_name   => ($self->input eq $self->_file_handle? '' : $self->input),
		file_handle => $self->_file_handle,
		logger      => $self->logger,
		@args
	);
}

sub _coerce_field_list {
	my ($list)= @_;
	defined $list and ref $list eq 'ARRAY' or croak "'fields' must be a non-empty arrayref";
	my @list= @$list; # clone it, to make sure we don't unexpectedly alter the caller's data
	for (@list) {
		if (!ref $_) {
			$_= Data::RecordExtractor::Field->new({ name => $_ });
		} elsif (ref $_ eq 'HASH') {
			my %args= %$_;
			# "isa" alias for the 'type' attribute
			$args{type}= delete $args{isa} if defined $args{isa} && !defined $args{type};
			$_= Data::RecordExtractor::Field->new(\%args)
		} else {
			croak "Can't coerce '$_' to a Field object"
		}
	}
	return \@list;
}

=head1 METHODS

=head2 detect_input_format

This is used internally to detect the format of a file, but you can call it manually if you
like.  The first argument (optional) is a file name, and the second argument (also optional)
is the first few hundred bytes of the file.  Missing arguments will be pulled from L</input>
if possible.  The return value is the best guess of module name and constructor arguments that
should be used to parse the file.  However, this doesn't guarantee such module actually exists
or is installed; it might just echo the file extension back to you.

=cut

sub detect_input_format {
	my ($self, $filename, $magic)= @_;
	# Detect filename if not supplied
	if (!defined $filename) {
		my $input= $self->input;
		$filename= '';
		$filename= "$input" if defined $input and (!ref $input || ref($input) =~ /path|file/i);
	}
	my ($suffix)= ($filename =~ /\.([^.]+)$/);
	$suffix= defined $suffix? uc($suffix) : '';
	# Load first block of file, unless supplied
	my $fpos;
	if (!defined $magic) {
		my $fh= $self->_file_handle;
		$fpos= $fh->tell;
		if (defined $fpos && $fpos >= 0) {
			$fh->read($magic, 4096);
			$fh->seek($fpos, 0) or croak "seek: $!";
		} else {
			$magic= '';
		}
	}

	# Excel is obvious so check it first.  This handles cases where an excel file is
	# erroneously named ".csv" and sillyness like that.
	return ( 'XLSX' ) if $magic =~ /^PK(\x03\x04|\x05\x06|\x07\x08)/;
	return ( 'XLS'  ) if $magic =~ /^\xD0\xCF\x11\xE0/;

	# Else trust the file extension
	return $suffix if length $suffix;

	# Else probe some more...
	$log->debug("Probing file format because no filename suffix");
	length $magic or croak "Can't probe format. No filename suffix, and ".($fpos >= 0? "unseekable file handle" : "no content");

	my ($probably_csv, $probably_tsv)= (0,0);
	++$probably_csv if $magic =~ /^["']?[\w ]+["']?,/;
	++$probably_tsv if $magic =~ /^["']?[\w ]+["']?\t/;
	my $comma_count= ($magic =~ /,/g);
	my $tab_count= ($magic =~ /\t/g);
	my $eol_count= ($magic =~ /\n/g);
	++$probably_csv if $comma_count > $eol_count and $comma_count > $tab_count;
	++$probably_tsv if $tab_count > $eol_count and $tab_count > $comma_count;
	return 'CSV' if $probably_csv and $probably_csv > $probably_tsv;
	return 'TSV' if $probably_tsv and $probably_tsv > $probably_csv;

	croak "Can't determine file format";
}

=head2 find_table

Search through the input for the beginning of the records, identified by a header row matching
the various constraints defined in L</fields>.  If L</header_row_at> is undef, then this does
nothing and assumes success.

Returns a boolean of whether it succeeded.  This method does B<not> C<croak> on failure like
L</iterator> does, on the assumption that you want to handle them gracefully.
All diagnostics about the search are logged via L</logger>.

=cut

sub find_table {
	my $self= shift;
	return 1 if $self->has_col_map;
	my $colmap= $self->_find_table(0);
	$self->col_map($colmap) if $colmap;
	return !!$colmap;
}

sub _build_col_map {
	shift->_find_table(1);
}
sub _build_field_map {
	my $col_map= shift->col_map;
	my %fmap;
	for my $i (0 .. $#$col_map) {
		next unless $col_map->[$i];
		if ($col_map->[$i]->array) {
			push @{ $fmap{$col_map->[$i]->name} }, $i;
		} else {
			$fmap{$col_map->[$i]->name}= $i;
		}
	}
	\%fmap;
}

sub _write_log {
	my ($self, $level, $msg, @args)= @_;
	$level.='f' if @args;
	$self->logger->$level($msg, @args);
}

=head2 iterator

Begin iterating records.  If the table has not been located, then find it and C<croak> if it
can't be found.

There is only one iterator (because there is only one L</input> file handle) so multiple calls
to this method return the same L<iterator object|Data::RecordExtractor::Iterator>.
You may be able to L<seek|Data::RecordExtractor::Iterator/seek> on the iterator if the
L<decoder|Data::RecordExtractor::Decoder> or L</input> handle support that.

=cut

sub _build_iterator {
	my $self= shift;
	my $data_iter= $self->decoder->iterator;
	my $col_map= $self->col_map;
	my $fields= $self->fields;
   
	my (@col_idx, @field_names, @trim_idx, $class);
	if ($self->record_class eq 'ARRAY') {
		my $field_map= $self->field_map;
		@col_idx= map { defined $_? $_ : 999999999 } map { $col_map->{$_->name} } @$fields;
		@trim_idx= grep { $fields->[$_]->trim } 0 .. $#$fields;
	}
	else {
	  @field_names= keys %$col_map;
	  @col_idx= values %$col_map;
	  my %trim_names= map { $_->trim? ( $_->name => 1 ) : () } @$fields;
	  @trim_idx= map { $col_map->{$_} } grep { $trim_names{$_} } @field_names;
	  $class= $self->record_class
		 unless 'HASH' eq $self->record_class;
	}
	my $sub= sub {
	  my $row= $data_iter->(\@col_idx);
	  for (@{$row}[@trim_idx]) {
		 $_ =~ s/\s+$//;
		 $_ =~ s/^\s+//;
	  }
	  return $row unless @field_names;
	  my %rec;
	  @rec{@field_names}= @$row;
	  return $class? $class->new(\%rec) : \%rec;
	};
	return Data::RecordExtractor::RecordIterator->new(
	  $sub,
	  {
		 data_iter => $data_iter
	  },
	);
}

sub _find_table {
	my ($self, $croak_on_fail)= @_;
	my $col_fields= $self->_find_table_in_current_dataset;
	while (!$col_fields && $self->decoder->iterator->next_dataset) {
		$col_fields= $self->_find_table_in_current_dataset;
	}
	return $col_fields if $col_fields or !$croak_on_fail;
	croak "Can't locate valid header";
}

sub _find_table_in_current_dataset {
	my ($self, $croak_on_fail)= @_;
	# If header_row_at is undef, then there is no header.
	# Ensure static_field_order, then set up columns.
	my @fields= $self->field_list;
	my $header_at= $self->header_row_at;
	if (!defined $header_at) {
		unless ($self->static_field_order) {
			$self->_write_log('error', my $msg= "You must enable 'static_field_order' if there is no header row");
			croak $msg if $croak_on_fail;
			return;
		}
		return \@fields;
	}
	
	# If headers contain "\n", we need to collect multiple cells per column
	my $row_accum= max map { 1+($_->header_regex =~ /\n/g) } @fields;
	
	my ($start, $end)= ref $header_at? @$header_at : ( $header_at, $header_at );
	my $iter= $self->decoder->iterator;
	my @rows;
	
	# If header_row_at doesn't start at 1, seek forward
	push @rows, $iter->() for 1..$start-1;
	
	# Scan through the rows of the dataset up to the end of header_row_at, accumulating rows so that
	# multi-line regexes can match.
	my @cols;
	row: for ($start..$end) {
		push @rows, $iter->() || return; # if undef, we reached end of dataset
		splice @rows, 0, @rows-$row_accum; # only need to retain $row_accum number of rows
		my $vals= $row_accum == 1? $rows[-1]
			: [ map { my $c= $_; join("\n", map $_->[$c], @rows) } 0 .. $#{$rows[-1]} ];
		# If static field order, look for headers in sequence
		if ($self->static_field_order) {
			for my $i (0 .. $#fields) {
				next if $vals->[$i] =~ $fields[$i]->header_regex;
				
				# Field header doesn't match.  Start over on next row.
				$self->_write_log('debug', 'Missing field %s on %s', $fields[$i]->name, $iter->position);
				next row;
			}
			# found a match for every field!
			@cols= @fields;
			last row;
		}
		# else search for each header
		else {
			my %col_map;
			my @free_fields= grep { !$_->follows_list } @fields;
			my @follows_fields= grep { $_->follows_list } @fields;
			# Sort required first, to fail faster on non-matching rows
			for my $f (sort { $a->required? -1 : $b->required? 1 : 0 } @free_fields) {
				my $hr= $f->header_regex;
				my @found= grep { $vals->[$_] =~ $hr } 0 .. $#$vals;
				if (@found == 1) {
					if ($col_map{$found[0]}) {
						$self->_write_log('warn', 'Field %s and %s both match at %s', $f->name, $col_map{$found[0]}->name, $iter->position);
						next row;
					}
					$col_map{$found[0]}= $f;
				}
				elsif (@found > 1) {
					if ($f->array) {
						# Array columns may be found more than once
						$col_map{$_}= $f for @found;
					} else {
						$self->_write_log('warn', 'Field %s matches more than one column at %s', $f->name, $iter->position);
						next row;
					}
				}
				elsif ($f->required) {
					$self->_write_log('debug', 'No match for required field %s at %s', $f->name, $iter->position);
					next row;
				}
				# else Not required, and not found
			}
			# Need to have found at least one column (even if none required)
			unless (keys %col_map) {
				$self->_write_log('debug', 'No fields matched at %s', $iter->position);
				next row;
			}
			# Now, check for any of the 'follows' fields, some of which might also be 'required'.
			if (@follows_fields) {
				my %following;
				my %found;
				for my $i (0 .. $#$vals) {
					if ($col_map{$i}) {
						%following= ( $col_map{$i}->name => $col_map{$i} );
					} else {
						my $val= $vals->[$i];
						my @match;
						for my $f (@follows_fields) {
							next unless grep $following{$_}, $f->follows_list;
							push @match, $f if $val =~ $f->header_regex;
						}
						if (@match == 1) {
							if ($found{$match[0]} && !$match[0]->array) {
								$self->_write_log('error', 'Field %s matches multiple columns at %s', $match[0]->name, $iter->position);
								next row;
							}
							$col_map{$i}= $match[0];
							$found{$match[0]}= $i;
							$following{$match[0]->name}= $match[0];
						}
						elsif (@match > 1) {
							$self->_write_log('error', 'Field %s and %s both match at %s', $match[0]->name, $match[1]->name, $iter->position);
							next row;
						}
						else {
							%following= ();
						}
					}
				}
				# Check if any of the 'follows' fields were required
				if (my @unfound= grep { !$found{$_} && $_->required } @follows_fields) {
					$self->_write_log('debug', 'No match for required field %s at %s', $_->name, $iter->position)
						for @unfound;
				}
			}
			# Now, if there are any un-claimed columns, handle per 'on_unknown_columns' setting.
			my @unclaimed= grep { !$col_map{$_} } 0 .. $#$vals;
			if (@unclaimed) {
				my $act= $self->on_unknown_columns;
				my $unknown_list= join(', ', map $vals->[$_], @unclaimed);
				$act= $act->($self, \@cols) if ref $act eq 'CODE';
				if ($act eq 'use') {
					$self->_write_log('warn', 'Ignoring unknown columns: %s', $unknown_list);
				} elsif ($act eq 'next') {
					$self->_write_log('warn', '%s would match except for unknown columns: %s',
						$iter->position, $unknown_list);
					next row;
				} elsif ($act eq 'die') {
					my $msg= "Header row includes unknown columns: $unknown_list";
					$self->_write_log('error', $msg);
					croak $msg;
				} else {
					croak "Invalid action '$act' for 'on_unknown_columns'";
				}
			}
			@cols= map $col_map{$_}, 0 .. $#$vals;
			last row;
		}
	}
	unless (@cols) {
		$self->_write_log('warn', 'No row in dataset matched full header requirements');
		return;
	}
	return \@cols;
}

1;
