# -*- mode: ruby -*-
# vi: set ft=ruby :

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  vagrant_default_provider = ENV['VAGRANT_DEFAULT_PROVIDER'] || "virtualbox"
  if vagrant_default_provider == nil
    puts "No default vagrant provider defined, using 'virtualbox'. To define set the environment variable 'VAGRANT_DEFAULT_PROVIDER'"
  end

  config.vm.box = "puppetlabs/centos-6.6-64-nocm"
  if vagrant_default_provider == "libvirt"
    config.vm.box_url = "http://kwok.cz/centos64.box"
    config.vm.provider :libvirt do |libvirt|
      libvirt.storage_pool_name = "default"
    end
  elsif vagrant_default_provider == "virtualbox"
  else
    raise('Unsupported vagrant provider')
  end

  config.omnibus.chef_version = "11.12.8"
  config.berkshelf.enabled = true

  config.vm.provision "chef_solo" do |chef|
    chef.json = {
      :postgresql => {
        :password => {
          :postgres => "postgres"
        }
      }
    }
    chef.add_recipe "postgresql::server"
    chef.add_recipe "arbalest"
    chef.add_recipe "arbalest::test"
  end
end
