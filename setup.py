# see https://github.com/karlicoss/pymplate for up-to-date reference


from setuptools import setup, find_namespace_packages # type: ignore


def main() -> None:
    # works with both ordinary and namespace packages
    pkgs = find_namespace_packages('src')
    pkg = min(pkgs) # lexicographically smallest is the correct one usually?
    setup(
        name=pkg,
        use_scm_version={
            'version_scheme': 'python-simplified-semver',
            'local_scheme': 'dirty-tag',
        },
        setup_requires=['setuptools_scm'],

        # otherwise mypy won't work
        # https://mypy.readthedocs.io/en/stable/installed_packages.html#making-pep-561-compatible-packages
        zip_safe=False,

        packages=pkgs,
        package_dir={'': 'src'},
        # necessary so that package works with mypy
        package_data={pkg: ['py.typed']},

        ## ^^^ this should be mostly automatic and not requiring any changes

        install_requires=[
            'more-itertools',
            'typing-extensions',
            'click'        , # nicer cli
            'plumbum'      , # nicer command composition/piping
            'python-magic' , # better mimetype detection (could be optional?)
            'logzero'      , # nicer logging
            'lxml'         , # TODO for xml module, make optional later
            'orjson'       , # faster json processing, could be optional
            'kompress'     , # for compressed files processing (could be optional if they don't use compressed files?)

            # vvv example of git repo dependency
            # 'repo @ git+https://github.com/karlicoss/repo.git',

            # vvv  example of local file dependency. yes, DUMMY is necessary for some reason
            # 'repo @ git+file://DUMMY/path/to/repo',
        ],
        extras_require={
            'testing': [
                'pytest',
                'ruff',
                'mypy', 'lxml',  # lxml for mypy coverage report
            ],
            'zstd'   : ['kompress[zstd]'],
            'HPI': [  # for bleanser.modules.hpi
                'HPI', # pypi version
                # 'HPI @ git+https://github.com/karlicoss/hpi.git',   # uncomment to test against github version (useful for one-off CI run)
                # 'HPI @ git+file:///DUMMY/path/to/local/hpi@branch', # uncomment to test against version on the disc
                # note: sometimes you need to use file://DUMMY?? wtf?..
            ],
        },


        # this needs to be set if you're planning to upload to pypi
        # url='',
        # author='',
        # author_email='',
        # description='',

        # Rest of the stuff -- classifiers, license, etc, I don't think it matters for pypi
        # it's just unnecessary duplication
    )


if __name__ == '__main__':
    main()

# TODO
# from setuptools_scm import get_version
# https://github.com/pypa/setuptools_scm#default-versioning-scheme
# get_version(version_scheme='python-simplified-semver', local_scheme='no-local-version')
