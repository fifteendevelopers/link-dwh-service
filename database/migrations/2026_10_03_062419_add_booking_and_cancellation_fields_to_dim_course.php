<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Run the migrations.
     */
    public function up(): void
    {
        Schema::connection('mysql')->table('Dim_Course', function (Blueprint $table) {
            $table->boolean('Is_Cancelled')->default(false)->after('Year_Group');
            $table->text('Cancellation_Reason')->nullable()->after('Is_Cancelled');
            $table->boolean('Has_Confirmed_Booked')->default(false)->after('Cancellation_Reason');
            $table->string('Confirmed_By', 255)->nullable()->after('Has_Confirmed_Booked');
            $table->string('Confirmed_By_Email', 255)->nullable()->after('Confirmed_By');
            $table->timestamp('Confirmed_At')->nullable()->after('Confirmed_By_Email');
            $table->integer('Provisional_Booked')->nullable()->after('Confirmed_At');
            $table->integer('Booked')->nullable()->after('Provisional_Booked');
            $table->integer('Adults')->nullable()->after('Booked');
            $table->integer('Children')->nullable()->after('Adults');
            $table->json('Attendees_Overridden_Metadata')->nullable()->after('Children');
            $table->unsignedBigInteger('Attendees_Overridden_By')->nullable()->after('Attendees_Overridden_Metadata');
            $table->timestamp('Attendees_Overridden_At')->nullable()->after('Attendees_Overridden_By');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('Dim_Course', function (Blueprint $table) {
            $table->dropColumn([
                'Is_Cancelled',
                'Cancellation_Reason',
                'Has_Confirmed_Booked',
                'Confirmed_By',
                'Confirmed_By_Email',
                'Confirmed_At',
                'Provisional_Booked',
                'Booked',
                'Adults',
                'Children',
                'Attendees_Overridden_Metadata',
                'Attendees_Overridden_By',
                'Attendees_Overridden_At',
            ]);
        });
    }
};
