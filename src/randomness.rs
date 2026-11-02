//! Secret material is independent of sortable public identifiers.
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use rand::{
    TryRng,
    rngs::{SysError, SysRng},
};

/// @cc [owner:ghuntley,label:security] secrets-use-os-entropy
/// Secret bytes MUST come directly from the operating system CSPRNG, never UUIDs,
/// timestamps or seeded test generators. Entropy failure MUST return an error.
pub fn random_bytes<const N: usize>() -> Result<[u8; N], SysError> {
    let mut bytes = [0_u8; N];
    SysRng.try_fill_bytes(&mut bytes)?;
    Ok(bytes)
}

/// New bearer secrets carry 256 random bits; their textual encoding is URL safe.
pub fn new_secret() -> Result<String, SysError> {
    Ok(URL_SAFE_NO_PAD.encode(random_bytes::<32>()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secrets_encode_all_32_bytes_without_padding() {
        let secret = new_secret().unwrap();
        assert_eq!(secret.len(), 43);
        assert_eq!(URL_SAFE_NO_PAD.decode(secret).unwrap().len(), 32);
    }
}
