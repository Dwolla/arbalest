#
# Cookbook Name:: arbalest
# Recipe:: test
#

node.default["arbalest"]["home"] = "/vagrant"

execute "python setup.py install" do
    cwd node["arbalest"]["home"]
end

execute "python setup.py test" do
    cwd node["arbalest"]["home"]
end

execute "pip install discover" do
    cwd node["arbalest"]["home"]
end

execute "python -m discover -s integration" do
    cwd node["arbalest"]["home"]
end
