use strict;
use warnings;
#use ExtUtils::MakeMaker;
use inc::Module::Install;

WriteMakefile(
    NAME                => 'DBIx::Simple::Procedure',
    AUTHOR              => q{Al Newkirk <al.newkirk@awnstudio.com>},
    VERSION_FROM        => 'lib/DBIx/Simple/Procedure.pm',
    ABSTRACT_FROM       => 'lib/DBIx/Simple/Procedure.pm',
    ($ExtUtils::MakeMaker::VERSION >= 6.3002
      ? ('LICENSE'=> 'perl')
      : ()),
    PL_FILES            => {},
    PREREQ_PM => {
        'Test::More' => 0,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'DBIx-Simple-Procedure-*' },
);
