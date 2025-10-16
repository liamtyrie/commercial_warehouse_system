use std::collections::HashMap;

use chrono::{Duration, Utc};
use jsonwebtoken::{encode, EncodingKey, Header};
use pwhash::bcrypt;
use serde::{Deserialize, Serialize};

// Data struct for the claims within the JWT
#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,
    exp: usize,
}

pub struct AuthService {
    // Representing a database with a HashMap, until I've figured out what DB I want to use.
    user_store: HashMap<String, String>,
}

impl AuthService {
    #[allow(dead_code)]
    fn new_with_mock_user(employee_id: &str, password: &str) -> Self {
        let hash = bcrypt::hash(password).expect("Failed to hash password");
        let mut user_store = HashMap::new();
        user_store.insert(employee_id.to_string(), hash);
        AuthService { user_store }
    }

    pub fn verify_and_issue_token(
        &self,
        employee_id: &str,
        password: &str,
    ) -> Result<String, AuthError> {
        let stored_hash = self
            .user_store
            .get(employee_id)
            .ok_or(AuthError::UserNotFound)?;

        if !bcrypt::verify(password, stored_hash) {
            return Err(AuthError::InvalidCredentials);
        }

        let claims = Claims {
            sub: employee_id.to_string(),
            exp: (Utc::now() + Duration::hours(24)).timestamp() as usize,
        };

        let secret = "super-secret-key";
        encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(secret.as_ref()),
        )
        .map_err(|_| AuthError::TokenCreationError)
    }
}

#[derive(Debug, PartialEq)]
pub enum AuthError {
    UserNotFound,
    InvalidCredentials,
    TokenCreationError,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_return_jwt_for_valid_credentials() {
        let employee_id = "AMZ-1138";
        let password = "correct-password";
        let auth_service = AuthService::new_with_mock_user(employee_id, password);

        let result = auth_service.verify_and_issue_token(employee_id, password);

        assert!(result.is_ok());

        assert!(result.unwrap().split('.').count() == 3);
    }
}
