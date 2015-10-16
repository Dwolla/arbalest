name              "arbalest"
maintainer        "Dwolla, Inc."
maintainer_email  "dev@dwolla.com"
description       "arbalest"
version           "1.5.0"
recipe            "arbalest", "Integration test"

%w{ubuntu debian fedora suse amazon}.each do |os|
  supports os
end

%w{redhat centos scientific oracle}.each do |el|
  supports el, ">= 6.0"
end
