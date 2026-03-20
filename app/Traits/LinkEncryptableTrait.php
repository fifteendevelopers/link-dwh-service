<?php
namespace App\Traits;

use Illuminate\Support\Facades\Crypt;
trait LinkEncryptableTrait
{
    /**
     * If the attribute is in the encryptable array
     * then decrypt it.
     *
     * @param  $key
     *
     * @return $value
     */
    public function getAttribute($key)
    {
        $value = parent::getAttribute($key);

        if ($value === null) {
            return null;
        }

        if (in_array($key, $this->encryptable)) {
            $value = Crypt::decrypt($value);
        }
        return $value;

    }
    /**
     * If the attribute is in the encryptable array
     * then encrypt it.
     *
     * @param $key
     * @param $value
     */
    public function setAttribute($key, $value)
    {
        if (in_array($key, $this->encryptable)) {
            $value = Crypt::encrypt($value);
        }
        return parent::setAttribute($key, $value);
    }
    /**
     * When need to make sure that we iterate through
     * all the keys.
     *
     * @return array
     */
    public function attributesToArray()
    {
        $attributes = parent::attributesToArray();
        foreach ($this->casts as $key => $value) {
            if($value == 'encrypt') {
                if (isset($attributes[$key]) && $attributes[$key] !== '' && !is_null($attributes[$key])) {
                    $attributes[$key] = Crypt::decrypt($attributes[$key]);
                }
            }
        }
        return $attributes;
    }
}
