#
# Cookbook Name:: arbalest
# Recipe:: default
#

package "gcc"
package "python-devel"
package "python-setuptools"
package "postgresql-devel"

bash "psycopg2" do
    code <<-EOH
    easy_install pip
    pip install psycopg2
    EOH
end
