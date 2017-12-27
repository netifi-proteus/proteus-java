#include <memory>

#include "java_generator.h"
#include <google/protobuf/compiler/code_generator.h>
#include <google/protobuf/compiler/plugin.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/zero_copy_stream.h>

static string JavaPackageToDir(const string& package_name) {
  string package_dir = package_name;
  for (size_t i = 0; i < package_dir.size(); ++i) {
    if (package_dir[i] == '.') {
      package_dir[i] = '/';
    }
  }
  if (!package_dir.empty()) package_dir += "/";
  return package_dir;
}

class JavaProteusGenerator : public google::protobuf::compiler::CodeGenerator {
 public:
  JavaProteusGenerator() {}
  virtual ~JavaProteusGenerator() {}

  virtual bool Generate(const google::protobuf::FileDescriptor* file,
                        const string& parameter,
                        google::protobuf::compiler::GeneratorContext* context,
                        string* error) const {
    std::vector<std::pair<string, string> > options;
    google::protobuf::compiler::ParseGeneratorParameter(parameter, &options);

    java_proteus_generator::ProtoFlavor flavor =
        java_proteus_generator::ProtoFlavor::NORMAL;

    bool disable_version = false;
    for (size_t i = 0; i < options.size(); i++) {
      if (options[i].first == "lite") {
        flavor = java_proteus_generator::ProtoFlavor::LITE;
      } else if (options[i].first == "noversion") {
        disable_version = true;
      }
    }

    string package_name = java_proteus_generator::ServiceJavaPackage(file);
    string package_filename = JavaPackageToDir(package_name);
    for (int i = 0; i < file->service_count(); ++i) {
      const google::protobuf::ServiceDescriptor* service = file->service(i);

      string interface_filename = package_filename + service->name() + ".java";
      std::unique_ptr<google::protobuf::io::ZeroCopyOutputStream> interface_file(
          context->Open(interface_filename));
      java_proteus_generator::GenerateInterface(
          service, interface_file.get(), flavor, disable_version);

      string client_filename = package_filename
          + java_proteus_generator::ClientClassName(service) + ".java";
      std::unique_ptr<google::protobuf::io::ZeroCopyOutputStream> client_file(
          context->Open(client_filename));
      java_proteus_generator::GenerateClient(
          service, client_file.get(), flavor, disable_version);

      string server_filename = package_filename
          + java_proteus_generator::ServerClassName(service) + ".java";
      std::unique_ptr<google::protobuf::io::ZeroCopyOutputStream> server_file(
          context->Open(server_filename));
      java_proteus_generator::GenerateServer(
          service, server_file.get(), flavor, disable_version);
    }
    return true;
  }
};

int main(int argc, char* argv[]) {
  JavaProteusGenerator generator;
  return google::protobuf::compiler::PluginMain(argc, argv, &generator);
}
