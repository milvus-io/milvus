//
// Copyright (C) 2015-2020 Yahoo Japan Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include	"NGT/Version.h"

void
NGT::Version::get(std::ostream &os) 
{
  os << "  Version:" << NGT::Version::getVersion() << std::endl;
  os << "  Built date:" << NGT::Version::getBuildDate() << std::endl;
  os << "  The last git tag:" << Version::getGitTag() << std::endl;
  os << "  The last git commit hash:" << Version::getGitHash() << std::endl;
  os << "  The last git commit date:" << Version::getGitDate() << std::endl;
}

const std::string 
NGT::Version::getVersion() 
{
  return NGT_VERSION; 
}

const std::string 
NGT::Version::getBuildDate() 
{
  return NGT_BUILD_DATE; 
}

const std::string 
NGT::Version::getGitHash() 
{ 
  return NGT_GIT_HASH; 
}

const std::string 
NGT::Version::getGitDate() 
{ 
  return NGT_GIT_DATE;
}

const std::string 
NGT::Version::getGitTag()
{ 
  return NGT_GIT_TAG; 
}

