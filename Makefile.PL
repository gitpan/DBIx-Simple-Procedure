use strict;
use warnings;
use inc::Module::Install;
#use ExtUtils::MakeMaker;

WriteMakefile(
    NAME                => 'DBIx::Simple::Procedure',
    AUTHOR              => q{Al Newkirk <al.newkirk@awnstudio.com>},
    VERSION_FROM        => 'lib/DBIx/Simple/Procedure.pm',
    ABSTRACT_FROM       => 'lib/DBIx/Simple/Procedure.pm',
    (1
      ? ('LICENSE'=> 'perl')
      : ()),
    PL_FILES            => {},
    PREREQ_PM => {
        'Test::More' => 0,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'DBIx-Simple-Procedure-*' },
);
