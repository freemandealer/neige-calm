//! A Mac private directory has no Linux mount or mount-namespace identity.
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PlatformIdentity {
    Linux(u64),
    MacOsPrivateDirectory,
}

// Preserve the released Linux JSON numbers explicitly. Binary ordinals belong to the
// negotiated execution envelope; Mac facts are never decoded as Linux zeroes.
#[derive(Serialize, Deserialize)]
enum Wire {
    Linux(u64),
    MacOsPrivateDirectory,
}
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum Json {
    Linux(u64),
    Platform(Wire),
}
impl Serialize for PlatformIdentity {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let wire = match self {
            Self::Linux(id) => Wire::Linux(*id),
            Self::MacOsPrivateDirectory => Wire::MacOsPrivateDirectory,
        };
        if serializer.is_human_readable() {
            match wire {
                Wire::Linux(id) => Json::Linux(id),
                other => Json::Platform(other),
            }
            .serialize(serializer)
        } else {
            wire.serialize(serializer)
        }
    }
}
impl<'de> Deserialize<'de> for PlatformIdentity {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = if deserializer.is_human_readable() {
            match Json::deserialize(deserializer)? {
                Json::Linux(id) => Wire::Linux(id),
                Json::Platform(wire) => wire,
            }
        } else {
            Wire::deserialize(deserializer)?
        };
        Ok(match wire {
            Wire::Linux(id) => Self::Linux(id),
            Wire::MacOsPrivateDirectory => Self::MacOsPrivateDirectory,
        })
    }
}
