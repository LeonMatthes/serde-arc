use core::{any::Any, sync::atomic::AtomicU64};
use lazy_static::lazy_static;
use serde::{
    de::{VariantAccess, Visitor},
    ser::Serializer,
    Deserialize, Serialize,
};
use std::{
    collections::{hash_map::Entry, HashMap},
    marker::PhantomData,
    sync::{atomic::Ordering, Arc, Mutex, Weak},
};

type Arena = Mutex<HashMap<u64, Weak<dyn Any + Send + Sync>>>;

const NO_ID: u64 = 0;

lazy_static! {
    static ref ARENA: Arena = Mutex::new(HashMap::new());
    static ref SERIALIZE_ID: AtomicU64 = AtomicU64::new(NO_ID + 1);
}

#[derive(Serialize, Deserialize)]
pub struct SArcInner<T> {
    value: T,
    id: AtomicU64,
}

impl<T> Drop for SArcInner<T> {
    // To avoid memory leaks caused by keeping Weak pointers to each SArcInner
    // alive in the Arena, remove them from the the Arena when they're dropped.
    fn drop(&mut self) {
        let id = self.id.load(Ordering::Acquire);
        if id != NO_ID {
            let mut arena = ARENA.lock().unwrap();
            if let Entry::Occupied(entry) = arena.entry(id) {
                // If the entry can still be upgraded, the entry stored in the arena is not
                // the one being currently deleted.
                // But we probably can't do anything to fix this here.
                // So leave the value alone if it's still upgradeable.
                if Weak::upgrade(entry.get()).is_none() {
                    entry.remove();
                }
            }
        }
    }
}

pub struct SArc<T> {
    inner: Arc<SArcInner<T>>,
}

impl<T> SArc<T> {
    pub fn new(value: T) -> Self {
        SArc {
            inner: Arc::new(SArcInner {
                id: AtomicU64::new(NO_ID),
                value,
            }),
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn clone(sarc: &Self) -> Self {
        SArc {
            inner: Arc::clone(&sarc.inner),
        }
    }

    pub fn ptr_eq(this: &Self, other: &Self) -> bool {
        Arc::ptr_eq(&this.inner, &other.inner)
    }
}

impl<T> std::ops::Deref for SArc<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner.value
    }
}

const SERDE_VALUE: u32 = 0;
const SERDE_ID: u32 = 1;

impl<T> Serialize for SArc<T>
where
    T: Serialize + Any + Send + Sync + 'static,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut new_id = NO_ID;
        let try_assign_id =
            self.inner
                .id
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current_id| {
                    if current_id == 0 {
                        new_id = SERIALIZE_ID.fetch_add(1, Ordering::AcqRel);
                        // if we wrap around
                        if new_id == NO_ID {
                            panic!("Maximum number of sendable Arcs reached!");
                        }
                        Some(new_id)
                    } else {
                        // We already have an id.
                        None
                    }
                });
        match try_assign_id {
            Ok(_previous_id) => {
                assert_eq!(_previous_id, 0);
                // We set the it, so populate it and serialize the value + id.
                {
                    let mut arena = ARENA.lock().unwrap();
                    arena.insert(new_id, Arc::<SArcInner<T>>::downgrade(&self.inner));
                }

                serializer.serialize_newtype_variant("SArc", SERDE_VALUE, "Value", &*self.inner)
            }
            // We didn't set the id, so we already have one.
            Err(id) => serializer.serialize_newtype_variant("SArc", SERDE_ID, "Id", &id),
        }
    }
}

impl<'de, T> Deserialize<'de> for SArc<T>
where
    T: Deserialize<'de> + Any + Send + Sync,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        enum Field {
            Id,
            Value,
        }

        use serde::de::Error;

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct FieldVisitor {}

                impl<'de> Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                        write!(formatter, "variant identifier")
                    }

                    fn visit_u64<E: Error>(self, value: u64) -> Result<Self::Value, E> {
                        match value as u32 {
                            SERDE_ID => Ok(Field::Id),
                            SERDE_VALUE => Ok(Field::Value),
                            _ => Err(Error::invalid_value(
                                serde::de::Unexpected::Unsigned(value),
                                &"variant index for Id or Value",
                            )),
                        }
                    }
                    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
                    where
                        E: Error,
                    {
                        match value {
                            "Id" => Ok(Field::Id),
                            "Value" => Ok(Field::Value),
                            _ => Err(Error::unknown_variant(value, VARIANTS)),
                        }
                    }
                    fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
                    where
                        E: Error,
                    {
                        match value {
                            b"Id" => Ok(Field::Id),
                            b"Value" => Ok(Field::Value),
                            _ => {
                                let value = &String::from_utf8_lossy(value);
                                Err(Error::unknown_variant(value, VARIANTS))
                            }
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor {})
            }
        }

        struct SArcVisitor<T> {
            _data: PhantomData<T>,
        }

        impl<'de, T: Deserialize<'de> + Any + Send + Sync> Visitor<'de> for SArcVisitor<T> {
            type Value = SArc<T>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(formatter, "an SArc enum of either Id or Value")
            }

            fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::EnumAccess<'de>,
            {
                match data.variant()? {
                    (Field::Id, variant) => {
                        let id = VariantAccess::newtype_variant::<u64>(variant)?;
                        let arena = ARENA.lock().unwrap();
                        let inner = arena
                            .get(&id)
                            .ok_or(Error::custom("ID couldn't be found!"))?;
                        let inner = inner
                            .upgrade()
                            .ok_or(Error::custom(format!("SArc '{id}' is no longer alive!")))?;
                        let inner = inner
                            .downcast::<SArcInner<T>>()
                            .map_err(|_| Error::custom("SArc points to the wrong type!"))?;

                        Ok(SArc { inner })
                    }
                    (Field::Value, variant) => {
                        let inner = variant.newtype_variant::<SArcInner<T>>()?;
                        let mut arena = ARENA.lock().unwrap();
                        match arena.entry(inner.id.load(Ordering::Acquire)) {
                            // TODO does it make sense to overwrite this if it's faulty?
                            Entry::Occupied(mut occupied) => {
                                match occupied
                                    .get()
                                    .upgrade()
                                    .and_then(|inner| inner.downcast::<SArcInner<T>>().ok())
                                {
                                    None => {
                                        let inner = Arc::new(inner);
                                        occupied.insert(Arc::<SArcInner<T>>::downgrade(&inner));
                                        Ok(SArc { inner })
                                    }
                                    Some(inner) => Ok(SArc { inner }),
                                }
                            }
                            Entry::Vacant(vacant) => {
                                let inner = Arc::new(inner);
                                vacant.insert(Arc::<SArcInner<T>>::downgrade(&inner));
                                Ok(SArc { inner })
                            }
                        }
                    }
                }
            }
        }

        const VARIANTS: &[&str] = &["Value", "Id"];

        deserializer.deserialize_enum("SArc", VARIANTS, SArcVisitor { _data: PhantomData })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn serialize<T: Serialize>(t: &T) -> String {
        serde_json::to_string(t).unwrap()
    }
    #[test]
    fn serialize_works() {
        let initial_arc: SArc<i32> = SArc::new(15);
        let second_arc = SArc::clone(&initial_arc);
        serialize(&(initial_arc, second_arc));
    }

    #[test]
    fn deserialize_works() {
        let initial_arc: SArc<i64> = SArc::new(15);
        let second_arc = SArc::clone(&initial_arc);
        let arcs = (initial_arc, second_arc);
        let serialized1 = serialize(&arcs);
        let serialized2 = serialize(&arcs);

        let (first, second): (SArc<i64>, SArc<i64>) = serde_json::from_str(&serialized1).unwrap();
        let (third, fourth): (SArc<i64>, SArc<i64>) = serde_json::from_str(&serialized2).unwrap();

        assert!(SArc::ptr_eq(&arcs.0, &first));
        assert!(SArc::ptr_eq(&first, &second));
        assert!(SArc::ptr_eq(&second, &third));
        assert!(SArc::ptr_eq(&third, &fourth));
    }

    #[test]
    fn multi_threaded_deserialize() {
        #[derive(Serialize, Deserialize)]
        struct X {
            value: i32,
        }

        let initial_arc = SArc::new(X { value: 5 });
        std::thread::scope(|scope| {
            for _ in 0..50 {
                scope.spawn(|| {
                    let serialized = serde_json::to_string(&initial_arc).unwrap();
                    let de_arc: SArc<X> = serde_json::from_str(&serialized).unwrap();
                    assert!(SArc::ptr_eq(&de_arc, &initial_arc));
                });
            }
        });
    }

    #[test]
    fn deserialization_missing_id() {
        let arc: SArc<i32> = SArc::new(5);

        // The arc data will only be serialized the first time.
        // serialized2 therefore only contains the id, not the actual value.
        let _serialized1 = serialize(&arc);
        let serialized2 = serialize(&arc);

        // as we're currently serializing and deserializing within a single arena,
        // the serialization has already filled the arc into the arena.
        // So let's remove them for now.
        {
            let mut arena = ARENA.lock().unwrap();
            arena.remove(&arc.inner.id.load(Ordering::Acquire));
        }

        let result: Result<SArc<i32>, _> = serde_json::from_str(&serialized2);
        assert!(result.is_err());
        let err = result.map(|_| ()).unwrap_err();
        let err = format!("{err}");
        assert_eq!(err, "ID couldn't be found!");
    }
}
