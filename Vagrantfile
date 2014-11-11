# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu/trusty64"
  
  config.ssh.shell = "bash -c 'BASH_ENV=/etc/profile exec bash'"
  config.vm.provision :shell, :path => 'bootstrap.sh'
  
  config.vm.network :forwarded_port, host: 60010 , guest: 60010 
  config.vm.network :forwarded_port, host: 60030 , guest: 60030 

  config.vm.provider "virtualbox" do |v|
    v.memory = 4096
    v.cpus = 2
  end 	
end
