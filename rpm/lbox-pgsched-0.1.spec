Name:           lbox-pgsched
Summary:        PG scheduer
Version:        0.1
Release:        %{dist}.01

Group:          Applications/Databases
License:        proprietary
URL:            https://trac.linuxbox.cz/pgsched
Source:         %{name}-%{version}.tgz
BuildRoot:      %{_tmppath}/%{name}-root-%(%{__id_u} -n)
BuildArch:      noarch

Requires:       python >= 2.4 python-psycopg2-90 python-lbasync

%define prog_dir /usr/libexec/pgsched/
%define sql_dir /usr/pgsql-9.0/share/lbox/
%define doc_dir /usr/share/doc/%{name}-%{version}/

%description
PG scheduler alias pgsched is a minimal python daemon providing cron/at/init
functionality at a PostgreSQL server.

%prep
%setup 

%build

%install
mkdir -m 0755 -p "%{buildroot}%{prog_dir}" "%{buildroot}%{sql_dir}" "%{buildroot}%{doc_dir}" "%{buildroot}/etc/init.d/"
install --mode 0755 init-script "%{buildroot}/etc/init.d/pgsched"
install --mode 0755 pgsched.py "%{buildroot}%{prog_dir}"
install --mode 0644 sql/*.sql "%{buildroot}%{sql_dir}"
install --mode 0644 README.markdown "%{buildroot}%{doc_dir}"

%clean
rm -rf %{buildroot}

%post
chkconfig --add pgsched || :

%preun
# uninstall
if [ $1 = 0 ]; then
    /etc/init.d/pgsched stop >/dev/null 2>&1 || :
    chkconfig --del pgsched || :
fi

%postun
# upgrade
if [ $1 = 1 ]; then
    /etc/init.d/pgsched restart >/dev/null 2>&1
fi

%files
%defattr(-,root,root,-)
%attr(0755,root,root)%{prog_dir}pgsched.py
%attr(0644,root,root)%{prog_dir}pgsched.pyc
%attr(0644,root,root)%{prog_dir}pgsched.pyo
%attr(0644,root,root)%{sql_dir}pgsched.sql
%attr(0644,root,root)%{sql_dir}uninstall_pgsched.sql
%attr(0644,root,root)%{doc_dir}README.markdown
%attr(0755,root,root)/etc/init.d/pgsched
