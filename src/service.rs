use cubeos_service::*;

service_macro!{
    use failure::Error;
    subsystem::FileService {
        mutation: Download => fn download(&self, source_path: String, target_path: String) -> Result<()>;
        mutation: Upload => fn upload(&self, source_path: String, target_path: String) -> Result<()>;
        mutation: CleanUp => fn cleanup(&self, hash: Option<String>) -> Result<()>;
    }
}