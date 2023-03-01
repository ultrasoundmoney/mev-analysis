pub fn hex_to_option(hex: String) -> Option<String> {
    if hex == "0x" {
        None
    } else {
        Some(hex)
    }
}
